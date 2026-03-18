# gmail-auto-label (Rust)

[![Documentation](https://img.shields.io/badge/docs-docs.rs-blue)](https://docs.rs/crate/gmail-auto-label/latest)
[![License](https://img.shields.io/github/license/lichtcui/gmail-auto-label)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)
[![Crates.io](https://img.shields.io/crates/d/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)

基于 `gog` + `codex` 的 Gmail 自动分类打标工具（Rust 版）。  
这是中文文档。

🌐 Languages: [🇺🇸 English](README.md) · [🇨🇳 简体中文](README_ZH.md)

## 功能描述

- 自动扫描收件箱线程，并按业务语义打 Gmail 标签
- 缓存优先分类（记忆 + 可复用规则），减少重复调用大模型
- 缓存未命中时调用 Codex 分类，并把规则回写到本地缓存
- 自动创建缺失标签，并按标签批量回写邮件线程
- 支持打标后自动归档（移出 `INBOX`），也可用 `--keep-inbox` 保留收件箱
- 活跃标签超限时自动压缩合并（公开参数为 `--max-labels`，默认合并到 `其他通知`）
- 内置 Gmail 限流处理（自动重试 + 指数退避）
- 支持 `--dry-run` 演练模式，预览动作但不写入

## 前置条件

- 已安装并登录 `gog`
- 已安装 `codex` 命令（默认使用 `codex exec`）
- 已安装 Rust 工具链

## gog 配置

1. 安装 `gog`（按你的系统选择安装方式）。
2. 登录：

```bash
gog auth login
```

3. 验证 Gmail 可访问：

```bash
gog gmail labels list --no-input --json
```

4. 查看本地账号（确认账号名）：

```bash
gog auth list
```

5. 多账号场景运行时可指定账号：

```bash
cargo run -- --account your-account-name
```

说明：程序所有 Gmail 操作都通过 `gog` 执行。若认证缺失或权限不足，程序会在运行时报错。

### gog 排错建议

1. 检查当前认证状态：

```bash
gog auth status
```

2. 快速验证 Gmail API 可读：

```bash
gog gmail search "in:inbox" --max 1 --no-input --json
```

3. 若 token 或权限失效，重新登录：

```bash
gog auth login
```

## 构建

```bash
cargo build --release
```

可执行文件：

```bash
./target/release/gmail-auto-label
```

## 发布到 crates.io 后安装

发布完成后可直接安装：

```bash
cargo install gmail-auto-label
```

可选：指定版本安装

```bash
cargo install gmail-auto-label --version 0.1.1
```

## 常用用法

1. 单轮处理（默认 20 封）：

```bash
cargo run -- --limit 20
```

2. 演练模式（不落地写入）：

```bash
cargo run -- --dry-run --limit 20
```

3. 轮询模式（每 5 分钟一轮）：

```bash
cargo run -- --watch 300
```

4. 仅打标签，不归档（保留收件箱）：

```bash
cargo run -- --keep-inbox
```

## 关键参数

- `--limit`：每轮最多处理数量，默认 `20`
- `--watch`：按秒轮询，例如 `--watch 300`
- `--account`：指定 gog 账号名
- `--dry-run`：只打印操作，不执行写入
- `--max-labels`：最大活跃标签数，默认 `10`
- `--keep-inbox`：处理后不移出收件箱

## 高级参数

这些参数仍兼容保留，但默认隐藏，一般不需要手动设置：

- `--codex-cmd`
- `--cache-file`
- `--merged-label`
- 旧版兼容：`--loop` + `--interval` 仍可用，但推荐统一改成 `--watch`

内置反馈文件格式（内部路径固定为 `/tmp/gmail_auto_label_feedback.json`）：

```json
[
  {
    "event_id": "evt-20260318-001",
    "rule_id": "rule_sha256_id",
    "verdict": "bad",
    "ts": 1773800000
  }
]
```

说明：
- `event_id` 需唯一，重复/回放事件会被跳过。
- `ts` 为 Unix 秒时间戳，超过内置反馈时效的过期事件会被跳过。

## 查看帮助

```bash
cargo run -- --help
```
