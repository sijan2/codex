use std::path::Path;
use std::path::PathBuf;

use chrono::SecondsFormat;
use chrono::Utc;
use codex_git_utils::collect_git_info;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::EventPersistenceMode;
use codex_rollout::persisted_rollout_items;
use codex_state::ThreadMetadata;
use codex_state::ThreadMetadataBuilder;

use crate::CreateThreadParams;
use crate::ResumeThreadParams;
use crate::ThreadEventPersistenceMode;
use crate::ThreadMetadataUpdate;

/// Prepares canonical rollout items and explicit metadata updates for a live thread.
///
/// `LiveThread` owns one handler per active thread so metadata inference stays above the raw
/// `ThreadStore` append API.
pub(crate) struct ThreadMetadataHandler {
    thread_id: ThreadId,
    metadata: ThreadMetadata,
    latest_session_meta: Option<SessionMetaLine>,
    initial_item: Option<RolloutItem>,
    event_persistence_mode: EventPersistenceMode,
    memory_mode: ThreadMemoryMode,
    dynamic_tools: Option<Vec<codex_protocol::dynamic_tools::DynamicToolSpec>>,
}

/// Result of applying metadata policy to an incoming live-thread operation.
///
/// `items` are the canonical rollout items to append, and `update` is the explicit metadata delta
/// to pass to `ThreadStore::apply_thread_metadata` after those items are accepted.
pub(crate) struct PreparedThreadMetadata {
    pub(crate) items: Vec<RolloutItem>,
    pub(crate) update: ThreadMetadataUpdate,
}

impl ThreadMetadataHandler {
    pub(crate) async fn for_create(params: &CreateThreadParams) -> Self {
        let created_at = Utc::now();
        let cwd = params.metadata.cwd.clone().unwrap_or_default();
        let git = protocol_git_info(cwd.as_path()).await;
        let session_meta = SessionMeta {
            id: params.thread_id,
            forked_from_id: params.forked_from_id,
            timestamp: created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            cwd: cwd.clone(),
            originator: params.originator.clone(),
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            source: params.source.clone(),
            thread_source: params.thread_source,
            agent_nickname: params.source.get_nickname(),
            agent_role: params.source.get_agent_role(),
            agent_path: params.source.get_agent_path().map(Into::into),
            model_provider: Some(params.metadata.model_provider.clone()),
            base_instructions: Some(params.base_instructions.clone()),
            dynamic_tools: if params.dynamic_tools.is_empty() {
                None
            } else {
                Some(params.dynamic_tools.clone())
            },
            memory_mode: memory_mode_as_str(params.metadata.memory_mode).map(str::to_string),
        };
        let session_meta_line = SessionMetaLine {
            meta: session_meta,
            git,
        };
        let metadata = metadata_from_session_meta(
            &session_meta_line,
            created_at,
            params.metadata.model_provider.as_str(),
        );
        Self {
            thread_id: params.thread_id,
            metadata,
            latest_session_meta: Some(session_meta_line.clone()),
            initial_item: Some(RolloutItem::SessionMeta(session_meta_line)),
            event_persistence_mode: event_persistence_mode(params.event_persistence_mode),
            memory_mode: params.metadata.memory_mode,
            dynamic_tools: (!params.dynamic_tools.is_empty()).then(|| params.dynamic_tools.clone()),
        }
    }

    pub(crate) fn for_resume(params: &ResumeThreadParams) -> Self {
        let created_at = Utc::now();
        let mut builder = ThreadMetadataBuilder::new(
            params.thread_id,
            params.rollout_path.clone().unwrap_or_default(),
            created_at,
            SessionSource::Unknown,
        );
        builder.model_provider = Some(params.metadata.model_provider.clone());
        builder.cwd = params.metadata.cwd.clone().unwrap_or_default();
        let metadata = builder.build(params.metadata.model_provider.as_str());
        let mut handler = Self {
            thread_id: params.thread_id,
            metadata,
            latest_session_meta: None,
            initial_item: None,
            event_persistence_mode: event_persistence_mode(params.event_persistence_mode),
            memory_mode: params.metadata.memory_mode,
            dynamic_tools: None,
        };
        if let Some(history) = params.history.as_deref() {
            handler.observe_items(history, /*updated_at_override*/ None);
        }
        handler
    }

    pub(crate) fn take_initial_metadata(&mut self) -> Option<PreparedThreadMetadata> {
        let item = self.initial_item.take()?;
        self.observe_items(std::slice::from_ref(&item), Some(self.metadata.created_at));
        Some(PreparedThreadMetadata {
            items: vec![item],
            update: self.current_update(),
        })
    }

    pub(crate) fn prepare_items(
        &mut self,
        items: &[RolloutItem],
    ) -> Option<PreparedThreadMetadata> {
        let persisted = persisted_rollout_items(items, self.event_persistence_mode);
        if persisted.is_empty() {
            return None;
        }
        let updates_session_metadata = persisted.iter().any(|item| {
            matches!(item, RolloutItem::SessionMeta(meta_line) if meta_line.meta.id == self.thread_id)
        });
        self.observe_items(persisted.as_slice(), /*updated_at_override*/ None);
        let mut update = self.current_update();
        if !updates_session_metadata {
            update.memory_mode = None;
            update.dynamic_tools = None;
        }
        Some(PreparedThreadMetadata {
            items: persisted,
            update,
        })
    }

    pub(crate) fn prepare_metadata_patch(
        &mut self,
        patch: &crate::ThreadMetadataPatch,
    ) -> PreparedThreadMetadata {
        let mut items = Vec::new();
        if (patch.memory_mode.is_some() || patch.git_info.is_some())
            && let Some(mut session_meta) = self.latest_session_meta.clone()
        {
            if let Some(memory_mode) = patch.memory_mode {
                session_meta.meta.memory_mode = Some(memory_mode_to_str(memory_mode).to_string());
            }
            if let Some(git_info) = patch.git_info.clone() {
                session_meta.git = Some(resolve_git_info_patch(session_meta.git, git_info));
            }
            items.push(RolloutItem::SessionMeta(session_meta));
            self.observe_items(items.as_slice(), /*updated_at_override*/ None);
        }
        let mut update = ThreadMetadataUpdate {
            updated_at: Some(Utc::now()),
            ..Default::default()
        };
        if let Some(name) = patch.name.clone() {
            update.name = Some(Some(name));
        }
        if let Some(memory_mode) = patch.memory_mode {
            update.memory_mode = Some(memory_mode);
        }
        if patch.git_info.is_some() {
            update.git_info = Some(protocol_git_info_from_metadata(&self.metadata));
        }
        PreparedThreadMetadata { items, update }
    }

    fn observe_items(
        &mut self,
        items: &[RolloutItem],
        updated_at_override: Option<chrono::DateTime<Utc>>,
    ) {
        let updated_at = updated_at_override.unwrap_or_else(Utc::now);
        for item in items {
            let default_provider = self.metadata.model_provider.clone();
            codex_state::apply_rollout_item(&mut self.metadata, item, default_provider.as_str());
            match item {
                RolloutItem::SessionMeta(meta_line) if meta_line.meta.id == self.thread_id => {
                    self.latest_session_meta = Some(meta_line.clone());
                    if let Some(memory_mode) = meta_line.meta.memory_mode.as_deref()
                        && let Some(mode) = parse_memory_mode(memory_mode)
                    {
                        self.memory_mode = mode;
                    }
                    self.dynamic_tools = meta_line.meta.dynamic_tools.clone();
                }
                RolloutItem::EventMsg(EventMsg::UserMessage(_))
                | RolloutItem::EventMsg(EventMsg::TokenCount(_))
                | RolloutItem::TurnContext(_) => {}
                RolloutItem::SessionMeta(_)
                | RolloutItem::EventMsg(_)
                | RolloutItem::ResponseItem(_)
                | RolloutItem::Compacted(_) => {}
            }
        }
        self.metadata.updated_at = updated_at;
    }

    fn current_update(&self) -> ThreadMetadataUpdate {
        ThreadMetadataUpdate {
            rollout_path: (!self.metadata.rollout_path.as_os_str().is_empty())
                .then(|| self.metadata.rollout_path.clone()),
            forked_from_id: None,
            preview: (!self.metadata.title.is_empty()).then(|| self.metadata.title.clone()),
            name: None,
            model_provider: Some(self.metadata.model_provider.clone()),
            model: Some(self.metadata.model.clone()),
            reasoning_effort: Some(self.metadata.reasoning_effort),
            created_at: Some(self.metadata.created_at),
            updated_at: Some(self.metadata.updated_at),
            source: Some(parse_session_source(self.metadata.source.as_str())),
            thread_source: Some(self.metadata.thread_source),
            agent_nickname: Some(self.metadata.agent_nickname.clone()),
            agent_role: Some(self.metadata.agent_role.clone()),
            agent_path: Some(self.metadata.agent_path.clone()),
            cwd: Some(self.metadata.cwd.clone()),
            cli_version: Some(self.metadata.cli_version.clone()),
            approval_mode: parse_json_field(self.metadata.approval_mode.as_str()),
            sandbox_policy: parse_json_field(self.metadata.sandbox_policy.as_str()),
            token_usage: None,
            first_user_message: Some(self.metadata.first_user_message.clone()),
            git_info: Some(protocol_git_info_from_metadata(&self.metadata)),
            memory_mode: Some(self.memory_mode),
            dynamic_tools: Some(self.dynamic_tools.clone().unwrap_or_default()),
        }
    }
}

fn metadata_from_session_meta(
    session_meta_line: &SessionMetaLine,
    created_at: chrono::DateTime<Utc>,
    default_provider: &str,
) -> ThreadMetadata {
    let mut builder = ThreadMetadataBuilder::new(
        session_meta_line.meta.id,
        PathBuf::new(),
        created_at,
        session_meta_line.meta.source.clone(),
    );
    builder.thread_source = session_meta_line.meta.thread_source;
    builder.agent_nickname = session_meta_line.meta.agent_nickname.clone();
    builder.agent_role = session_meta_line.meta.agent_role.clone();
    builder.agent_path = session_meta_line.meta.agent_path.clone();
    builder.model_provider = session_meta_line.meta.model_provider.clone();
    builder.cwd = session_meta_line.meta.cwd.clone();
    builder.cli_version = Some(session_meta_line.meta.cli_version.clone());
    if let Some(git) = session_meta_line.git.as_ref() {
        builder.git_sha = git.commit_hash.as_ref().map(|sha| sha.0.clone());
        builder.git_branch = git.branch.clone();
        builder.git_origin_url = git.repository_url.clone();
    }
    builder.build(default_provider)
}

fn event_persistence_mode(mode: ThreadEventPersistenceMode) -> EventPersistenceMode {
    match mode {
        ThreadEventPersistenceMode::Limited => EventPersistenceMode::Limited,
        ThreadEventPersistenceMode::Extended => EventPersistenceMode::Extended,
    }
}

async fn protocol_git_info(cwd: &Path) -> Option<GitInfo> {
    collect_git_info(cwd).await.map(|info| GitInfo {
        commit_hash: info.commit_hash,
        branch: info.branch,
        repository_url: info.repository_url,
    })
}

fn protocol_git_info_from_metadata(metadata: &ThreadMetadata) -> Option<GitInfo> {
    if metadata.git_sha.is_none()
        && metadata.git_branch.is_none()
        && metadata.git_origin_url.is_none()
    {
        return None;
    }
    Some(GitInfo {
        commit_hash: metadata
            .git_sha
            .as_deref()
            .map(codex_git_utils::GitSha::new),
        branch: metadata.git_branch.clone(),
        repository_url: metadata.git_origin_url.clone(),
    })
}

fn parse_session_source(value: &str) -> SessionSource {
    parse_json_field(value).unwrap_or(SessionSource::Unknown)
}

fn parse_json_field<T>(value: &str) -> Option<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str::<T>(&format!("\"{value}\"")).ok()
}

fn memory_mode_as_str(mode: ThreadMemoryMode) -> Option<&'static str> {
    match mode {
        ThreadMemoryMode::Enabled => None,
        ThreadMemoryMode::Disabled => Some("disabled"),
    }
}

fn memory_mode_to_str(mode: ThreadMemoryMode) -> &'static str {
    match mode {
        ThreadMemoryMode::Enabled => "enabled",
        ThreadMemoryMode::Disabled => "disabled",
    }
}

fn parse_memory_mode(value: &str) -> Option<ThreadMemoryMode> {
    match value {
        "enabled" => Some(ThreadMemoryMode::Enabled),
        "disabled" => Some(ThreadMemoryMode::Disabled),
        _ => None,
    }
}

fn resolve_git_info_patch(existing: Option<GitInfo>, patch: crate::GitInfoPatch) -> GitInfo {
    let existing_sha = existing
        .as_ref()
        .and_then(|git| git.commit_hash.as_ref().map(|sha| sha.0.clone()));
    let existing_branch = existing.as_ref().and_then(|git| git.branch.clone());
    let existing_origin_url = existing.and_then(|git| git.repository_url);
    GitInfo {
        commit_hash: patch
            .sha
            .unwrap_or(existing_sha)
            .as_deref()
            .map(codex_git_utils::GitSha::new),
        branch: patch.branch.unwrap_or(existing_branch),
        repository_url: patch.origin_url.unwrap_or(existing_origin_url),
    }
}

#[cfg(test)]
mod tests {
    use codex_protocol::ThreadId;
    use codex_protocol::dynamic_tools::DynamicToolSpec;
    use codex_protocol::models::BaseInstructions;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::RolloutItem;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::ThreadMemoryMode;
    use codex_protocol::protocol::UserMessageEvent;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;
    use crate::CreateThreadParams;
    use crate::GitInfoPatch;
    use crate::ResumeThreadParams;
    use crate::ThreadEventPersistenceMode;
    use crate::ThreadMetadataPatch;
    use crate::ThreadPersistenceMetadata;

    #[tokio::test]
    async fn initial_metadata_synthesizes_session_meta_and_explicit_update() {
        let temp = TempDir::new().expect("temp dir");
        let mut params = create_params(temp.path().to_path_buf());
        params.metadata.memory_mode = ThreadMemoryMode::Disabled;
        params.dynamic_tools = vec![DynamicToolSpec {
            namespace: Some("ns".to_string()),
            name: "lookup".to_string(),
            description: "lookup".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
            defer_loading: false,
        }];
        let mut handler = ThreadMetadataHandler::for_create(&params).await;

        let prepared = handler.take_initial_metadata().expect("initial metadata");

        assert_eq!(prepared.items.len(), 1);
        let RolloutItem::SessionMeta(session_meta) = &prepared.items[0] else {
            panic!("expected session metadata");
        };
        assert_eq!(session_meta.meta.id, params.thread_id);
        assert_eq!(session_meta.meta.memory_mode.as_deref(), Some("disabled"));
        assert_eq!(
            session_meta
                .meta
                .dynamic_tools
                .as_ref()
                .expect("dynamic tools")[0]
                .name,
            "lookup"
        );
        assert_eq!(
            prepared.update.memory_mode,
            Some(ThreadMemoryMode::Disabled)
        );
        assert_eq!(
            prepared.update.dynamic_tools.expect("dynamic tools update")[0].name,
            "lookup"
        );
        assert_eq!(
            prepared.update.cli_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn user_message_updates_first_message_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let mut handler =
            ThreadMetadataHandler::for_create(&create_params(temp.path().to_path_buf())).await;
        let _ = handler.take_initial_metadata();

        let prepared = handler
            .prepare_items(&[RolloutItem::EventMsg(EventMsg::UserMessage(
                UserMessageEvent {
                    message: "hello metadata".to_string(),
                    images: None,
                    local_images: Vec::new(),
                    text_elements: Vec::new(),
                },
            ))])
            .expect("prepared items");

        assert_eq!(prepared.items.len(), 1);
        assert_eq!(
            prepared.update.first_user_message.flatten().as_deref(),
            Some("hello metadata")
        );
        assert_eq!(prepared.update.preview.as_deref(), Some("hello metadata"));
        assert_eq!(prepared.update.memory_mode, None);
        assert_eq!(prepared.update.dynamic_tools, None);
    }

    #[tokio::test]
    async fn resume_observes_existing_session_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let mut create_params = create_params(temp.path().to_path_buf());
        create_params.metadata.memory_mode = ThreadMemoryMode::Disabled;
        let mut created = ThreadMetadataHandler::for_create(&create_params).await;
        let initial = created.take_initial_metadata().expect("initial metadata");
        let thread_id = create_params.thread_id;

        let resumed = ThreadMetadataHandler::for_resume(&ResumeThreadParams {
            thread_id,
            rollout_path: Some(temp.path().join("rollout.jsonl")),
            history: Some(initial.items),
            include_archived: true,
            metadata: create_params.metadata.clone(),
            event_persistence_mode: ThreadEventPersistenceMode::Limited,
        });

        let update = resumed.current_update();
        assert_eq!(update.memory_mode, Some(ThreadMemoryMode::Disabled));
        assert_eq!(
            update.cli_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn metadata_patch_synthesizes_session_meta_and_update() {
        let temp = TempDir::new().expect("temp dir");
        let mut handler =
            ThreadMetadataHandler::for_create(&create_params(temp.path().to_path_buf())).await;
        let _ = handler.take_initial_metadata();

        let prepared = handler.prepare_metadata_patch(&ThreadMetadataPatch {
            memory_mode: Some(ThreadMemoryMode::Disabled),
            git_info: Some(GitInfoPatch {
                sha: Some(Some("abcdef".to_string())),
                branch: Some(Some("main".to_string())),
                origin_url: Some(Some("https://example.com/repo.git".to_string())),
            }),
            ..Default::default()
        });

        assert_eq!(prepared.items.len(), 1);
        let RolloutItem::SessionMeta(session_meta) = &prepared.items[0] else {
            panic!("expected session metadata");
        };
        assert_eq!(session_meta.meta.memory_mode.as_deref(), Some("disabled"));
        assert_eq!(
            session_meta
                .git
                .as_ref()
                .and_then(|git| git.branch.as_deref()),
            Some("main")
        );
        assert_eq!(
            prepared.update.memory_mode,
            Some(ThreadMemoryMode::Disabled)
        );
        assert_eq!(
            prepared
                .update
                .git_info
                .flatten()
                .and_then(|git| git.repository_url),
            Some("https://example.com/repo.git".to_string())
        );
    }

    fn create_params(cwd: std::path::PathBuf) -> CreateThreadParams {
        CreateThreadParams {
            thread_id: ThreadId::new(),
            forked_from_id: None,
            source: SessionSource::Exec,
            originator: "test_originator".to_string(),
            thread_source: None,
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            metadata: ThreadPersistenceMetadata {
                cwd: Some(cwd),
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
            event_persistence_mode: ThreadEventPersistenceMode::Limited,
        }
    }
}
