use std::path::PathBuf;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::ThreadMemoryMode;
use tokio::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::AppendThreadItemsParams;
use crate::ApplyThreadMetadataParams;
use crate::CreateThreadParams;
use crate::LoadThreadHistoryParams;
use crate::LocalThreadStore;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadMetadataPatch;
use crate::ThreadMetadataUpdate;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::thread_metadata_handler::PreparedThreadMetadata;
use crate::thread_metadata_handler::ThreadMetadataHandler;

/// Handle for an active thread's persistence lifecycle.
///
/// `LiveThread` keeps lifecycle decisions with the caller while delegating storage details to
/// [`ThreadStore`]. Local stores may use a rollout file internally and remote stores may use a
/// service, but session code should only need this handle for the active thread.
#[derive(Clone)]
pub struct LiveThread {
    thread_id: ThreadId,
    thread_store: Arc<dyn ThreadStore>,
    metadata_handler: Arc<Mutex<ThreadMetadataHandler>>,
    operation_semaphore: Arc<Semaphore>,
}

/// Owns a live thread while session initialization is still fallible.
///
/// If initialization returns early after persistence has been opened, dropping this guard discards
/// the live writer without forcing lazy in-memory state to become durable. Call [`commit`] once the
/// session owns the live thread for normal operation.
pub struct LiveThreadInitGuard {
    live_thread: Option<LiveThread>,
}

impl LiveThreadInitGuard {
    pub fn new(live_thread: Option<LiveThread>) -> Self {
        Self { live_thread }
    }

    pub fn as_ref(&self) -> Option<&LiveThread> {
        self.live_thread.as_ref()
    }

    pub fn commit(&mut self) {
        self.live_thread = None;
    }

    pub async fn discard(&mut self) {
        let Some(live_thread) = self.live_thread.take() else {
            return;
        };
        if let Err(err) = live_thread.discard().await {
            warn!("failed to discard thread persistence for failed session init: {err}");
        }
    }
}

impl Drop for LiveThreadInitGuard {
    fn drop(&mut self) {
        let Some(live_thread) = self.live_thread.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("failed to discard thread persistence for failed session init: no Tokio runtime");
            return;
        };
        handle.spawn(async move {
            if let Err(err) = live_thread.discard().await {
                warn!("failed to discard thread persistence for failed session init: {err}");
            }
        });
    }
}

impl LiveThread {
    pub async fn create(
        thread_store: Arc<dyn ThreadStore>,
        params: CreateThreadParams,
    ) -> ThreadStoreResult<Self> {
        let thread_id = params.thread_id;
        let metadata_handler =
            Arc::new(Mutex::new(ThreadMetadataHandler::for_create(&params).await));
        thread_store.create_thread(params).await?;
        let live_thread = Self {
            thread_id,
            thread_store,
            metadata_handler,
            operation_semaphore: Arc::new(Semaphore::new(1)),
        };
        Ok(live_thread)
    }

    pub async fn resume(
        thread_store: Arc<dyn ThreadStore>,
        params: ResumeThreadParams,
    ) -> ThreadStoreResult<Self> {
        let thread_id = params.thread_id;
        let metadata_handler = Arc::new(Mutex::new(ThreadMetadataHandler::for_resume(&params)));
        thread_store.resume_thread(params).await?;
        Ok(Self {
            thread_id,
            thread_store,
            metadata_handler,
            operation_semaphore: Arc::new(Semaphore::new(1)),
        })
    }

    pub async fn append_items(&self, items: &[RolloutItem]) -> ThreadStoreResult<()> {
        let _operation_permit = self.acquire_operation_permit().await?;
        let (initial, prepared) = {
            let mut handler = self.metadata_handler.lock().await;
            let Some(prepared) = handler.prepare_items(items) else {
                return Ok(());
            };
            let initial = handler.take_initial_metadata();
            (initial, prepared)
        };

        let initial_update = if let Some(initial) = initial {
            Some(self.append_prepared_items(initial).await?)
        } else {
            None
        };
        let update = self.append_prepared_items(prepared).await?;

        if let Some(update) = initial_update {
            self.apply_metadata_update(update).await?;
        }
        self.apply_metadata_update(update).await
    }

    async fn emit_initial_metadata(&self) -> ThreadStoreResult<()> {
        let prepared = {
            let mut handler = self.metadata_handler.lock().await;
            let Some(prepared) = handler.take_initial_metadata() else {
                return Ok(());
            };
            prepared
        };
        let update = self.append_prepared_items(prepared).await?;
        self.apply_metadata_update(update).await
    }

    async fn acquire_operation_permit(&self) -> ThreadStoreResult<OwnedSemaphorePermit> {
        self.operation_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ThreadStoreError::Internal {
                message: format!("live thread {} operation semaphore closed", self.thread_id),
            })
    }

    async fn append_prepared_items(
        &self,
        prepared: PreparedThreadMetadata,
    ) -> ThreadStoreResult<ThreadMetadataUpdate> {
        let update = prepared.update;
        self.thread_store
            .append_items(AppendThreadItemsParams {
                thread_id: self.thread_id,
                items: prepared.items,
            })
            .await?;
        Ok(update)
    }

    async fn apply_metadata_update(&self, update: ThreadMetadataUpdate) -> ThreadStoreResult<()> {
        self.thread_store
            .apply_thread_metadata(ApplyThreadMetadataParams {
                thread_id: self.thread_id,
                update,
            })
            .await
    }

    async fn append_prepared_metadata(
        &self,
        prepared: PreparedThreadMetadata,
    ) -> ThreadStoreResult<()> {
        let update = self.append_prepared_items(prepared).await?;
        self.apply_metadata_update(update).await
    }

    pub async fn persist(&self) -> ThreadStoreResult<()> {
        let _operation_permit = self.acquire_operation_permit().await?;
        self.emit_initial_metadata().await?;
        self.thread_store.persist_thread(self.thread_id).await
    }

    pub async fn flush(&self) -> ThreadStoreResult<()> {
        let _operation_permit = self.acquire_operation_permit().await?;
        self.emit_initial_metadata().await?;
        self.thread_store.flush_thread(self.thread_id).await
    }

    pub async fn shutdown(&self) -> ThreadStoreResult<()> {
        let _operation_permit = self.acquire_operation_permit().await?;
        self.emit_initial_metadata().await?;
        self.thread_store.shutdown_thread(self.thread_id).await
    }

    pub async fn discard(&self) -> ThreadStoreResult<()> {
        self.thread_store.discard_thread(self.thread_id).await
    }

    pub async fn load_history(
        &self,
        include_archived: bool,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        self.thread_store
            .load_history(LoadThreadHistoryParams {
                thread_id: self.thread_id,
                include_archived,
            })
            .await
    }

    pub async fn read_thread(
        &self,
        include_archived: bool,
        include_history: bool,
    ) -> ThreadStoreResult<StoredThread> {
        self.thread_store
            .read_thread(ReadThreadParams {
                thread_id: self.thread_id,
                include_archived,
                include_history,
            })
            .await
    }

    pub async fn update_memory_mode(
        &self,
        mode: ThreadMemoryMode,
        include_archived: bool,
    ) -> ThreadStoreResult<()> {
        self.update_metadata(
            ThreadMetadataPatch {
                memory_mode: Some(mode),
                ..Default::default()
            },
            include_archived,
        )
        .await?;
        Ok(())
    }

    pub async fn update_metadata(
        &self,
        patch: ThreadMetadataPatch,
        include_archived: bool,
    ) -> ThreadStoreResult<StoredThread> {
        if patch.name.is_some() || patch.memory_mode.is_some() || patch.git_info.is_some() {
            let _operation_permit = self.acquire_operation_permit().await?;
            let (initial, prepared) = {
                let mut handler = self.metadata_handler.lock().await;
                let initial = handler.take_initial_metadata();
                let prepared = handler.prepare_metadata_patch(&patch);
                (initial, prepared)
            };

            if let Some(initial) = initial {
                self.append_prepared_metadata(initial).await?;
            }
            if !prepared.items.is_empty() {
                self.append_prepared_metadata(prepared).await?;
            } else {
                self.apply_metadata_update(prepared.update).await?;
            }
        }

        self.thread_store
            .read_thread(ReadThreadParams {
                thread_id: self.thread_id,
                include_archived,
                include_history: false,
            })
            .await
    }

    /// Returns the live local rollout path for legacy local-only callers.
    ///
    /// Remote stores do not expose rollout files, so they return `Ok(None)`.
    pub async fn local_rollout_path(&self) -> ThreadStoreResult<Option<PathBuf>> {
        let Some(local_store) = self
            .thread_store
            .as_any()
            .downcast_ref::<LocalThreadStore>()
        else {
            return Ok(None);
        };
        local_store
            .live_rollout_path(self.thread_id)
            .await
            .map(Some)
    }
}
