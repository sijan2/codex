# Codex CLI Port to Android aarch64 Bionic

[OpenAI Codex CLI](https://github.com/openai/codex) cross-compiled for the **aarch64 Android (bionic)** userland.

Prebuilt binary: **[latest release](https://github.com/sijan2/codex/releases/latest)** — `codex-aarch64-linux-android`.

```sh
chmod +x codex-aarch64-linux-android
CODEX_HOME=/data/codex ./codex-aarch64-linux-android --version
```

Built for `aarch64-linux-android` (min API 35) with Rust 1.95.0 + NDK r27c. TLS via rustls; V8/code-mode and audio stubbed off.
