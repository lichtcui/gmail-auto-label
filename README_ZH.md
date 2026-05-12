# gmail-auto-label (Rust)

[![Documentation](https://img.shields.io/badge/docs-docs.rs-blue)](https://docs.rs/crate/gmail-auto-label/latest)
[![License](https://img.shields.io/github/license/lichtcui/gmail-auto-label)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)
[![Crates.io](https://img.shields.io/crates/d/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)

基于 DeepSeek API 的 Gmail 自动分类打标工具（Rust 版）。  
这是中文文档。

🌐 Languages: [🇺🇸 English](README.md) · [🇨🇳 简体中文](README_ZH.md)

## 功能描述

- 自动扫描收件箱线程，并按业务语义打 Gmail 标签
- **两阶段工作流**：IMAP 同步 + LLM 总结 → 批量分类 + 打标签（无需 Gmail API 调用）
- 缓存优先分类（记忆 + 可复用规则），减少重复调用大模型
- 缓存未命中时调用 LLM 分类，并把规则回写到本地缓存
- LLM 语义归并：标签超限时按语义分组，而非简单砍掉低频标签
- 标签由 Gmail IMAP 自动创建（无需独立 API 调用）
- 打标后自动归档（移出 `INBOX`）
- 支持机器可读的 JSON 输出（`--output json`）

## 前置条件

- DeepSeek API key（通过 `--api-key` 参数或 `DEEPSEEK_API_KEY` 环境变量设置）
- 已安装 Rust 工具链
- Gmail IMAP 应用专用密码（见下方 [IMAP 配置](#imap-配置)）

## IMAP 配置

Gmail 要求使用应用专用密码才能通过 IMAP 访问。普通密码不可用。

1. 在 https://myaccount.google.com/security 开启**两步验证**
2. 在 https://myaccount.google.com/apppasswords 生成一个**应用专用密码**（选择"Mail"作为应用）
3. 将该 16 位密码（空格可选）作为 `--imap-pass` 参数传入

## 构建

```bash
cargo build --release
```

可执行文件：

```bash
./target/release/gmail-auto-label
```

## 通过 crates.io 安装

可直接安装：

```bash
cargo install gmail-auto-label
```

安装后可直接执行命令：

```bash
gmail-auto-label --help
```

## LLM 配置

设置 DeepSeek API key（默认模型为 `deepseek-v4-flash`）：

```bash
# 通过环境变量（推荐）
export DEEPSEEK_API_KEY=sk-your-key-here

# 或通过命令行参数
gmail-auto-label --api-key sk-your-key-here
```

使用其他模型：

```bash
gmail-auto-label --model deepseek-v4-pro
```

## 使用方式

### 两阶段工作流

阶段 1 — 通过 IMAP 同步所有邮件并用 LLM 总结（零 Gmail API 读取调用）：

```bash
gmail-auto-label --sync --imap-user your@gmail.com --imap-pass your-app-password
```

> **IMAP 需使用应用专用密码**：Google 已不再接受普通密码登录 IMAP。
> 1. 在 https://myaccount.google.com/security 开启**两步验证**
> 2. 在 https://myaccount.google.com/apppasswords 生成一个**应用专用密码**（选择"Mail"作为应用）
> 3. 将该 16 位密码（空格可选）作为 `--imap-pass` 参数传入

可选限制同步的邮件数量：

```bash
gmail-auto-label --sync --imap-user your@gmail.com --imap-pass xxxx --sync-max 5000
```

阶段 2 — 从缓存读取摘要进行分类并通过 IMAP 批量打标签：

```bash
gmail-auto-label --from-cache --imap-user your@gmail.com --imap-pass your-app-password
```

该阶段通过 IMAP `STORE +X-GM-LABELS` 应用标签，零 Gmail API 调用。不存在的标签会被 Gmail IMAP 自动创建。

机器可读的 JSON 输出：

```bash
gmail-auto-label --output json
```

## 关键参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--api-key` | DeepSeek API key（或 `DEEPSEEK_API_KEY` 环境变量） | — |
| `--model` | DeepSeek 模型名 | `deepseek-v4-flash` |
| `--max-labels` | 最大活跃标签数 | `10` |
| `--output` | 输出格式（`text` \| `json`） | `text` |
| `--sync` | 运行 IMAP 同步阶段（拉取+总结） | — |
| `--from-cache` | 从先前同步的缓存数据中处理（需同时提供 `--imap-user` + `--imap-pass`） | — |
| `--imap-user` | IMAP 用户名（邮箱地址） | — |
| `--imap-pass` | IMAP 应用专用密码（普通密码不可用） | — |
| `--imap-host` | IMAP 服务器地址 | `imap.gmail.com` |
| `--imap-port` | IMAP 端口 | `993` |
| `--sync-max` | 最多同步的邮件数（0 = 不限） | `0` |

## 高级参数

这些参数仍兼容保留，但默认隐藏，一般不需要手动设置：

- `--cache-file`
- `--merged-label`

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

## 工作原理

### 数据流（两阶段模式）

```
阶段 1: IMAP 同步 (--sync)
  IMAP 收件箱 ──► 解析 MIME ──► 提取正文 ──► LLM 总结 ──► 本地缓存 (JSON)

阶段 2: 从缓存处理 (--from-cache)
  本地缓存 ──► 分类（缓存命中 → 学习规则 → LLM）──► IMAP STORE +X-GM-LABELS
```

阶段 2 完全读取本地缓存——零 Gmail API 调用。标签通过 IMAP `STORE +X-GM-LABELS` 写入，不存在标签会被 IMAP 自动创建。

### 分类优先级

1. 缓存 memo（精确匹配发件人+主题+摘要）
2. 学习得到的关键词规则（按命中数排序）
3. LLM 分类（DeepSeek API），自动提取规则回写

### 限流处理

- 读取操作（IMAP 同步，阶段 1）：零 Gmail API 调用
- 写入操作（标签创建/修改，阶段 2）：全部通过 IMAP —— 无 Gmail API 调用，无限流问题
- LLM 调用：自动重试，指数退避（1s, 2s, 4s），仅限瞬时故障

## 查看帮助

```bash
gmail-auto-label --help
```
