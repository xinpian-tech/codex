# Codex 多机器 Agent 基础设施需求

状态：需求整理稿

本文档记录 Codex fork 的当前需求基线，范围限定为需求、运行约束、数据绑定和生命周期。

## 1. 项目目标

### 1.1 核心目标

将 Codex 扩展为一个满足以下条件的 Agent 执行平台：

- 支持多机器执行。
- 支持本地和远端 Agent。
- 所有生产执行都在 Nix 环境内。
- 所有 Subagent 都由 tmux 管理。
- 所有 Agent 间通信都经过 tmux 输入输出。
- 支持多个 LLM Provider。
- 支持每个 Provider 下的多个账户。
- 支持 exploder、coder、hacker、leader 等可配置 Agent role。
- Agent 根据 role、职责和 Task 定向路由消息。
- 支持团队统一维护配置、Skills 和 Memory。
- 完整保存所有 Session。
- Codex fork 源码与团队运行状态分别由两个独立 Git repository 承载。
- 任意历史执行都可以追溯其机器、代码、配置、模型、账户和输出。

### 1.2 最小改动原则

必须优先复用 Codex 已有能力，包括：

- 配置加载与配置分层。
- Provider 和 Model 管理。
- Device Code 登录。
- `AuthManager`。
- Agent roles。
- Agent graph。
- Environment 和 exec-server。
- Goal。
- Skills。
- Memory。
- Hooks。
- Context compaction。
- `ThreadStore`。
- Rollout。
- Session resume、fork 和 revert。
- app-server v2。

新增能力构建在现有模块及其扩展点之上，通过新 crate 或独立模块实现，并减少对 `codex-core` 的修改。现有 rollout、session 和 app-server 接口的兼容性需要保留。

## 2. Git 仓库分离

### 2.1 Codex Source Repository

Codex Source Repository 承载 Codex fork 的源码和构建源码所需的内容，包括：

- Codex fork 源码。
- 构建 Codex fork 使用的 `flake.nix` 和 `flake.lock`。
- 源码对应的 Nix modules、packages 和构建文件。
- 源码开发所需的仓库级构建配置。

团队运行时配置、团队 Skills、团队 Memory、账户 token 和 Session 归属于 Team State Repository。

### 2.2 Team State Repository

Team State Repository 保存团队统一维护的运行状态，包括：

- 机器清单与机器 alias。
- Codex 运行配置。
- 组合 Codex Source Repository 与团队配置的运行 flake 及 lock file。
- Provider 和 Model 配置。
- 所有账户配置。
- 所有明文 token。
- OpenAI `auth.json`。
- API key、access token 和 refresh token。
- Agent roles 和路由策略。
- Rules、Hooks 和 MCP 配置。
- 团队 Skills 和 Memory。
- Task 配置。
- 所有 Session。
- Agent 间通信记录。
- tmux 输入输出。
- Workspace branch 和 commit 元数据。
- 构建及执行记录。

Team State Repository 的体积持续增长可以接受，完整留存和审计优先。

### 2.3 仓库边界

- Codex fork 源码归属于 Codex Source Repository。
- 团队 Skills、Memory、token 和 Session 归属于 Team State Repository。
- Codex Source Repository 的源码 branch 承载源码修改。
- Team State Repository 的 branch 和 refs 承载团队配置、知识、凭据与运行状态。
- Session 通过 repository 和 commit 引用源码仓库及实际 Task repository。
- 两个 repository 必须拥有彼此独立的 Git history、object database 和 remote。

### 2.4 Team State Repository refs

Team State Repository 当前约定的 refs 为：

```text
main
refs/codex/config
refs/codex/sessions/<root-session-id>
```

各 ref 的职责为：

```text
main
  机器定义、正式 Skills、正式 Memory 和团队声明式配置

refs/codex/config
  Codex 运行配置、Provider 配置、账户和明文凭据

refs/codex/sessions/*
  完整 Session、tmux I/O、rollout、事件和审计记录
```

Subagent 的工作 branch 保存在该 Agent 实际工作的 Task repository 中。Session 记录对应 repository、branch 和 commit。

### 2.5 可追溯性

任何生产执行都必须能够定位到：

```text
codex source repository
codex source commit
team state repository
team state commit
config commit
flake.lock hash
skill revision
memory revision
task repository
workspace commit
session ref
```

影响执行结果的临时参数和 Session override 必须保存在对应 Session 中，形成完整可恢复状态。

## 3. Nix 生产环境

### 3.1 生产执行环境

所有生产任务必须在 Nix 中执行。每个生产 Agent 的环境必须定位到具体 flake、derivation 和 store path。Nix generation 必须同时固定 Codex Source Repository revision 和 Team State Repository revision。

每次执行必须记录：

```text
codex source repository
codex source commit
team state repository
team state commit
flake URI
flake revision
flake.lock hash
nix system
derivation
store path
runtime environment
build output
```

### 3.2 完整配置进入 Nix

以下内容允许直接进入 Nix derivation、closure 和 `/nix/store`：

- Codex 配置。
- Provider 和 Model 配置。
- Skills 和 Memory。
- Agent roles。
- Rules、Hooks 和 MCP 配置。
- 明文 token。
- API key。
- OpenAI `auth.json`。
- refresh token 和 access token。

凭据以明文形式进入 Git、Nix derivation、Nix closure 和 `/nix/store`。

### 3.3 Config Generation

每次生产 Agent 启动必须绑定一个明确的配置 generation：

```text
ConfigGeneration
├── codex_source_repository
├── codex_source_commit
├── team_state_repository
├── team_state_commit
├── config_ref
├── config_commit
├── source_flake_lock_hash
├── state_flake_lock_hash
├── nix_system
├── config_derivation
├── config_store_path
├── effective_config_digest
├── provider_id
├── account_id
├── model_id
├── credential_revision
├── role_registry_revision
├── skills_revision
└── memory_revision
```

配置 generation 必须足以恢复 Agent 启动时实际加载的全部配置。

## 4. Task 底层解析

### 4.1 TaskSpec

Codex 在执行每个 Task 前必须形成明确的 `TaskSpec`：

```text
TaskSpec
├── task_id
├── objective
├── repo
├── source_commit
├── flake_uri
├── flake_revision
├── flake_lock_hash
├── target_machine
├── nix_system
├── build_required
├── build_target
├── runtime_command
├── dependencies
├── expected_outputs
└── assigned_agent
```

### 4.2 编译认知

Codex 必须在任务开始前明确：

- 任务是否需要编译。
- 如果需要编译，编译哪个项目。
- 使用哪个 flake attribute。
- 产生哪个 derivation。
- 预期输出位于哪个 store path。
- 编译结果用于哪个后续步骤。

需要编译时应形成类似以下信息：

```text
build_required = true
build_target = ".#codex"
```

非编译 Task 使用以下记录：

```text
build_required = false
build_target = none
```

Task 解析完成后进入命令执行阶段。

## 5. 机器模型

### 5.1 MachineId

机器的正式唯一标识直接使用操作系统 `hostid`：

```text
MachineId = normalized hostid
```

例如：

```text
4a17c0de
```

`hostid` 是唯一的 MachineId；hostname、IP、DNS 名称和机器 alias 作为机器元数据记录。

### 5.2 Machine Alias

Git 仓库维护 `hostid` 到人类可读 alias 的映射：

```toml
[hosts."4a17c0de"]
alias = "gpu-builder-01"
```

其中：

- `hostid` 是机器身份。
- alias 用于显示和选择。
- alias 可以修改。
- 修改 alias 时保留原有 MachineId。
- Session 同时记录当时的 MachineId、alias 和配置 commit。

### 5.3 MachineContext

Codex 必须始终知道自己和当前 Task 位于哪台机器：

```text
MachineContext
├── machine_id
├── machine_alias
├── machine_config_commit
├── hostname
├── nix_system
├── codex_store_path
├── workspace_path
├── tmux_session
├── tmux_window
├── tmux_pane
└── root_session_id
```

生产 Agent 的启动条件包含完整 `MachineContext`。

### 5.4 机器账户

所有机器都是沙盒机器，每台机器使用唯一的 root 账户运行相关服务和 Agent。

## 6. 本地与远端统一

### 6.1 相同 Agent 模型

本地和远端 Subagent 在功能上完全一致。两者都必须：

- 绑定 MachineId。
- 绑定 `TaskSpec`。
- 使用独立 worktree。
- 运行在 tmux 中。
- 使用动态 socket endpoint。
- 使用 Nix generation。
- 使用明确的 Provider、Account 和 Model。
- 保存完整 Session。
- 执行 commit/push Hooks。

本地和远端使用完全相同的 Agent 行为，endpoint 地址表达它们的运行位置。

### 6.2 统一传输路径

生产模式下，本地和远端 Subagent 的消息正文统一经过 tmux stdin、tmux stdout 和 tmux socket 协议。

## 7. Subagent tmux 管理

### 7.1 强制 tmux

每个 Subagent 必须：

- 是一个独立 Codex 进程。
- 在一台且仅一台机器上运行。
- 位于一个明确的 tmux session、window 和 pane。
- 具有稳定的 AgentId。
- 具有明确的 Parent Agent 或 root session 关系。
- 能够由 tmux runtime 重新发现。

### 7.2 tmux 布局

一个逻辑 root session 可以跨多台机器。每台参与机器维护对应的 tmux session：

```text
root-session-123

machine 4a17c0de:
  tmux session codex-root-session-123
    window agent-root-worker
    window agent-builder
    window agent-reviewer

machine 91bc204f:
  tmux session codex-root-session-123
    window agent-test
```

每个 Subagent 对应独立 window 和 pane。

### 7.3 AgentPlacement

```text
AgentPlacement
├── agent_id
├── parent_agent_id
├── root_session_id
├── machine_id
├── tmux_session
├── tmux_window
├── tmux_pane
└── tmux_endpoint
```

### 7.4 生命周期操作

以下操作必须通过 tmux runtime 完成：

- spawn。
- attach。
- wait。
- inspect。
- send input。
- receive output。
- interrupt。
- resume。
- terminate。
- reconnect。

生产 Subagent 的实际执行载体是 tmux 中的独立 Codex 进程。

### 7.5 Parent 或 Root Agent 断开

Parent 或 Root Agent 断开时：

- tmux server 继续运行。
- Subagent 继续运行。
- Subagent 的 worktree 保持原有状态。
- tmux 输出继续写入本地 spool。
- Parent 或 Root Agent 断开期间，Subagent 保持运行。
- 原 Parent、Root Agent 或其他接管该 Session 的 Agent 可以重新连接已有 tmux session。

### 7.6 独立生命周期

- 每个 Agent 拥有独立生命周期。
- `parent_agent_id` 表达任务关系；Agent 自身拥有进程生命周期。
- Parent Agent 退出后，Subagent 延续自己的生命周期。
- Agent 自己负责 checkpoint Hook、最终 Hook 和完成状态。
- tmux gateway 负责本机 tmux 生命周期与输入输出；Root 或 Parent Agent 承担调度角色。
- Team State Repository 保存可恢复的持久状态。
- 当前 Root 或 Parent Agent 在需要时承担编排角色。

## 8. tmux Gateway 和动态端口

### 8.1 Gateway

原生 tmux 使用本机 Unix socket。每台机器通过轻量 tmux gateway 将对应 tmux session 的输入输出暴露为明文 TCP socket。

通信路径为：

```text
Agent stdout
→ 本机 tmux Unix socket
→ 本机 tmux gateway
→ 明文 TCP socket
→ 目标机器 tmux gateway
→ 目标 tmux Unix socket
→ 目标 Agent stdin
```

### 8.2 动态端口

每个需要暴露的 tmux gateway 必须动态分配端口。启动时调用：

```text
bind(port = 0)
```

端口由操作系统分配。静态机器配置保存监听地址，运行时 Session 保存实际动态端口。

### 8.3 Endpoint 注册

Gateway 启动后发布：

```text
TmuxEndpoint
├── machine_id
├── root_session_id
├── address
└── dynamic_port
```

当前 Root Session 的 Agent Directory 维护运行时映射：

```text
(machine_id, root_session_id) → address:port
```

Gateway 重启时重新调用 `bind(port = 0)`，获得新的动态端口，报告新的 endpoint，并将端口变化记录到 Session。后续连接使用新的 endpoint。

### 8.4 本地 Endpoint

本地 Agent 同样使用动态端口：

```text
127.0.0.1:<dynamic-port>
```

本地 Agent 通过 gateway 连接 tmux。

### 8.5 网络传输

tmux 通信使用明文直接 TCP 和操作系统动态分配的端口。

## 9. Agent 间通信

### 9.1 唯一语义通道

所有 Agent 间语义内容都必须经过：

```text
发送 Agent stdout
→ 发送方 tmux
→ 发送方 gateway
→ 明文 socket
→ 接收方 gateway
→ 接收方 tmux
→ 接收 Agent stdin
```

该要求适用于：

- Parent Agent 向 Subagent 发任务。
- Subagent 向 Parent Agent 汇报。
- Subagent 之间协作。
- 本地 Agent 与远端 Agent 通信。
- 远端 Agent 之间通信。

### 9.2 消息传输边界

tmux stdin、tmux stdout 和 gateway socket 共同构成 Agent 消息正文的完整传输边界。现有 Agent 工具名称可以保留，生产实现将消息内容转换为 tmux 输入输出。

### 9.3 控制元数据

Tmux gateway 和 Agent Directory 处理非语义控制信息，例如 AgentId、MachineId、endpoint、tmux placement、sequence、ACK、消息长度、消息路由状态和 Agent 运行状态。Agent 生成消息正文，tmux 通道承载消息正文。

### 9.4 消息持久性

tmux 消息传输需要能够标识消息边界、发送和接收 Agent、消息顺序和交付状态。断线恢复过程识别待交付消息，并将完整消息写入 Session。`capture-pane` 用于观察和恢复，gateway control stream 用于可靠消息传输。

## 10. Role、Agent Directory 与路由认知

### 10.1 Role Registry

每个 Agent 必须具有明确的 role。Role 定义保存在 Team State Repository，并描述该类 Agent 的职责、输入、输出和路由关系。

Role Registry 支持以下已确认 role，并支持继续定义其他 role：

```text
exploder
coder
hacker
leader
```

配置中的 `RoleDefinition` 是 role 职责与路由关系的规范来源：

```text
RoleDefinition
├── role_id
├── responsibility
├── accepts_from_roles[]
├── routes_to_roles[]
├── accepted_input_kinds[]
├── produced_output_kinds[]
└── subscribed_events[]
```

Agent 根据自己的 `RoleDefinition` 判断应接收哪些输入、产生哪些输出、哪些 role 是合法路由候选，以及哪些状态事件与自己相关。

### 10.2 Session 级 Agent Directory

每个 Root Session 必须维护一个 Agent Directory。它是该 Session 状态中的 Agent 拓扑、职责、状态和路由规范视图。

```text
AgentDirectory
├── root_session_id
├── agents[]
└── updated_at
```

每个目录项至少包含：

```text
AgentDescriptor
├── agent_id
├── parent_agent_id
├── root_session_id
├── role
├── responsibility
├── task_id
├── status
├── machine_id
├── repo
├── commit
├── tmux_session
├── tmux_window
├── tmux_pane
├── tmux_endpoint
├── receives_from[]
└── sends_to[]
```

### 10.3 Agent 的自我认知

每个 Agent 必须始终知道：

- 自己的 AgentId。
- 自己的 role 和对应 `RoleDefinition`。
- 自己的 responsibility。
- 自己当前执行的 Task。
- 自己所在的 MachineId。
- 自己的 repo、branch、worktree 和 commit。
- 自己的 tmux session、window、pane 和 endpoint。
- 自己的 Parent Agent 和 Root Session。
- 自己允许从哪些 role 和 Agent 接收输入。
- 自己允许向哪些 role 和 Agent 发送结果。
- 自己订阅哪些状态事件。

### 10.4 对其他 Agent 的认知

每个 Agent 必须能够按需查询当前 Root Session 的完整 Agent Directory，并了解：

- 当前有哪些 Subagent。
- 哪些 Subagent 正在执行。
- 每个 Agent 的 role 和 responsibility。
- 每个 Agent 当前处理哪个 Task。
- 每个 Agent 工作在哪个 repository 和 commit。
- 每个 Agent 位于哪台机器和哪个 tmux placement。
- 每个 Agent 当前处于什么状态。
- 每个 Agent 从哪里接收工作。
- 每个 Agent 把结果发送给谁。

完整 Agent Directory 必须可以按需查询。模型可见的实时更新采用角色相关的定向通知。

Agent 至少使用以下状态表达当前生命周期：

```text
starting
running
waiting
finalizing
completed
failed
```

### 10.5 Bootstrap Context

Subagent 启动时，Parent Agent 必须通过该 Subagent 的 tmux stdin 发送 `AgentBootstrapContext`：

```text
AgentBootstrapContext
├── self
├── role_definition
├── parent
├── root_session_id
├── task
├── responsibility
├── input_sources[]
├── output_destinations[]
├── subscribed_events[]
└── agent_roster_snapshot
```

`agent_roster_snapshot` 在启动时提供当前 Agent 的身份、role、responsibility、Task 和状态概览。Subagent 在开始工作前必须明确自己是谁、负责什么、从哪里接收输入、应将结果发给谁，以及当前还有哪些 Agent 正在执行。

### 10.6 Directory 更新与定向通知

以下变化必须形成 Agent Directory 更新并持久化到 Team State Repository：

- Agent 创建或启动。
- Agent role 或 responsibility 变化。
- Agent Task 变化。
- Agent 状态变化。
- Agent MachineId 或 tmux placement 变化。
- Agent endpoint 变化。
- Agent repo 或 commit 变化。
- Agent 完成或进入 failed 状态。

每次 Directory 更新根据 Role Registry 中的 `subscribed_events`、`accepts_from_roles`、`routes_to_roles` 以及具体 Task 关系，解析出相关目标 Agent，并分别通过目标 Agent 的 tmux stdin 定向发送。

Agent 的模型上下文接收与其职责、Task、输入来源、输出目的地或订阅事件相关的状态更新。完整 Agent Directory 始终支持主动查询。

Tmux gateway 负责进程、placement 和 endpoint 等控制元数据；Agent 负责职责说明和消息正文。

### 10.7 发送前路由解析

Agent 发送消息前必须：

1. 读取自己的 `RoleDefinition`。
2. 根据消息类型、职责和 Task 得到合法的目标 role。
3. 查询当前 Agent Directory 中该 role 的候选 Agent。
4. 根据 Task 关系和 Agent 状态选择具体目标。
5. 将目标解析为具体 `to_agent_id`。
6. 读取目标 Agent 当前 MachineId、tmux placement 和 endpoint。
7. 将来源和目的地写入消息头。
8. 通过 tmux 和目标 gateway 定向发送。

Role 用于确定合法的路由方向，实际投递解析为具体 AgentId。每条消息使用明确的目标；多个目标对应多条分别带有 `to_agent_id` 的定向消息。

## 11. Agent 消息身份

### 11.1 强制消息头

每一条 Agent 间消息都必须声明：

- 发送者是谁。
- 发送者的职责。
- 输入来自哪台机器和哪个 Agent。
- 消息要发送到哪台机器和哪个 Agent。
- 发送者正在处理哪个 repository。
- 发送者当前位于哪个 commit。

最小消息结构为：

```text
AgentMessage
├── message_id
├── root_session_id
├── from_agent_id
├── from_role
├── from_machine_id
├── to_agent_id
├── to_role
├── to_machine_id
├── repo
├── commit
└── body
```

### 11.2 文本格式

接收 Agent 必须能在输入和模型上下文中看到：

```text
[agent-message]
message_id = "message-27"
root_session_id = "session-123"
from_agent_id = "agent-coder-03"
from_role = "coder"
from_machine_id = "4a17c0de"
to_agent_id = "agent-leader-02"
to_role = "leader"
to_machine_id = "91bc204f"
repo = "git@example.internal/team/project.git"
commit = "8f14e45fceea167a5a36dedd4bea2543c1b2a731"
[/agent-message]

构建已经完成，输出位于……
```

### 11.3 字段语义

`from_agent_id` 是发送者在 Root Session 中唯一的稳定 Agent 身份。`from_role` 描述发送者当前承担的职责。

`to_agent_id` 是已经通过 Agent Directory 解析出的具体接收 Agent。`to_role` 描述接收方职责。接收 Agent 必须确认 `to_agent_id` 与自己一致。

`from_machine_id` 和 `to_machine_id` 分别记录发送方和接收方的 MachineId。

`repo` 来自 Agent 的 Task 和 Workspace 绑定，表示该 Agent 实际工作的 Git repository。

Task repository 与 Codex Source Repository、Team State Repository 是独立概念。Agent 修改 Codex fork 时，Task repository 是 Codex Source Repository；Agent 整理团队 Skills 或 Memory 时，Task repository 是 Team State Repository；其他任务可以绑定其他 repository。

`commit` 是发送消息时该 worktree 当前 `HEAD`，使用完整 Git object ID，并对应已经成功 push 的 commit。

### 11.4 发送规则

- 每一条消息都包含完整的来源、目的地、repo 和 commit 字段。
- 发送方负责在写入自身 tmux stdout 前生成完整消息。
- 头部和正文必须走同一 tmux 通道。
- Tmux gateway 按原样转发 Agent 生成的消息头和正文。
- 完整字段集合是消息进入投递阶段的前置条件。
- Session 必须保存完整消息以及来源、目的地、repo 和 commit 字段。
- Agent 的 `HEAD` 改变后，下一条消息必须立即使用新 commit。
- Agent Directory 提供具体目标 Agent 后，消息进入投递阶段。
- 一条 `AgentMessage` 指定一个具体 `to_agent_id`；多个接收方对应多条独立消息。

## 12. Subagent Worktree 隔离

### 12.1 独立 Worktree

每个 Subagent 必须使用独立 Git worktree。路径位于所在机器的 `/tmp` 下：

```text
/tmp/codex/<root-session-id>/<agent-id>/<repo-name>/
```

例如：

```text
/tmp/codex/session-123/agent-builder-03/codex/
```

### 12.2 AgentWorkspace

```text
AgentWorkspace
├── agent_id
├── repo
├── base_commit
├── worktree_path
├── agent_branch
├── push_remote
├── current_commit
└── final_commit
```

### 12.3 独立 Branch

每个 Subagent 使用独立 branch：

```text
codex/<root-session-id>/<agent-id>
```

创建形式为：

```bash
git worktree add \
  -b codex/<root-session-id>/<agent-id> \
  /tmp/codex/<root-session-id>/<agent-id>/<repo-name> \
  <base-commit>
```

### 12.4 隔离规则

每个 Subagent：

- 所有修改发生在自己的 worktree。
- 主仓库工作目录保持为管理工作区。
- 每个 Agent worktree 由对应 Agent 独占。
- 每个 Agent checkout 独立工作 branch。
- Agent 通过已提交并 push 的 commit 交付修改。
- Agent 通过其他 Agent 已 push 的 commit 获取协作输入。
- 必须通过 commit 获取稳定的协作边界。

同一机器上的多个 Subagent 也必须使用不同 worktree。远端 Subagent 在远端机器的 `/tmp` 下创建自己的 worktree。

## 13. 修改后的 Commit/Push Hook

### 13.1 每次修改都必须 Checkpoint

每次会产生 Git 工作树修改的操作完成后，必须执行 checkpoint Hook：

```text
修改工作树
→ git status
→ git add -A
→ git commit
→ git push
→ 更新 Agent current_commit
→ 写入 Session
→ 才能继续汇报修改结果
```

“修改操作”包括所有通过 Agent 工具或 shell 命令产生仓库变化的操作。

### 13.2 Hook 行为

如果工作树存在变化，Hook 执行：

```bash
git add -A
git commit
git push
```

提交信息至少包含 AgentId 和 checkpoint 序号，例如：

```text
codex(agent-builder-03): checkpoint 17
```

### 13.3 Push 目标

- Commit 必须进入该 Subagent 的独立 branch。
- Push 必须推送该独立 branch。
- 每个 checkpoint 写入当前 Agent 的 branch。
- Push 后必须更新 Agent 的 `current_commit`。
- Session 必须记录 remote、branch、commit 和 push 结果。

### 13.4 Pending Checkpoint

Commit 和 push 全部成功后，checkpoint 进入 completed 状态。在此之前，checkpoint 保持 pending，并执行以下行为：

- 修改保持待持久化状态。
- 交付状态保持 pending。
- 完成消息引用最近一次已 push 的 commit。
- Agent 保留 Git 输出和当前 checkpoint 状态。
- Session 记录每次 commit 和 push 结果。
- 正常结束流程完成所有 pending checkpoint。

### 13.5 修改与通信顺序

任何涉及代码修改结果的 Agent 消息必须遵循：

```text
修改
→ commit
→ push
→ 获取完整 commit ID
→ 构造 AgentMessage
→ 通过 tmux 发送
```

消息中的 commit 必须在接收方收到消息时已经可以从 remote 获取。

## 14. Agent 结束 Hook

### 14.1 强制最终 Hook

Subagent 在以下操作前必须执行最终 Hook：

- 正常完成。
- 退出 Agent。
- 关闭 tmux window。
- 关闭 tmux session 中对应的 pane。
- 向 Parent Agent 发送最终结果。

### 14.2 最终 Hook 流程

```text
git status
→ 如果存在修改，git add -A
→ 如果存在暂存内容，git commit
→ git push
→ 确认当前 HEAD 已推送
→ 记录 final_commit
→ 写入 Session
→ 发送最终 AgentMessage
→ 允许 Agent 结束
```

即使没有新修改，最终 Hook 也必须执行 push、确认最终 `HEAD`、记录最终 commit，并记录 push 结果。

### 14.3 结束条件

Subagent 的正常结束状态满足：

- Worktree clean。
- 所有 commit 已 push。
- Final commit 已确定。
- 最终 Hook 已执行。
- 最终消息中的 commit 与已 push `HEAD` 一致。

最终结果必须包含：

```text
agent_id
repo
branch
worktree_path
base_commit
final_commit
push_remote
push_result
```

## 15. 多 Provider

### 15.1 Provider 独立选择

每个 Agent 都可以使用不同的 LLM Provider，包括 Root Agent、本地 Subagent、远端 Subagent、构建 Agent、Session 分析 Agent，以及 Skill 和 Memory 整理 Agent。

### 15.2 启动时确定

Agent 启动前必须确定：

```text
provider_id
account_id
model_id
credential_revision
```

Agent 启动绑定同时包含 Provider、Account、Model 和 credential revision。自动成本路由在 Agent 启动前解析为具体绑定。Agent 运行记录保存最终绑定结果。

### 15.3 InferenceBinding

```text
InferenceBinding
├── provider_id
├── account_id
├── model_id
├── credential_revision
└── selection_reason
```

该绑定是 `AgentExecution` 的必要组成部分。

## 16. 多账户

### 16.1 OpenAI 多账户

Codex 必须支持：

- 多个 OpenAI Device Code 登录账户。
- 为账户分配稳定的 `account_id`。
- 保存每个账户独立的 `auth.json`。
- 启动 Agent 时选择具体账户。
- 不同 Agent 同时使用不同 OpenAI 账户。

### 16.2 每 Agent 账户视图

可以为每个 Agent 生成只包含所选账户的独立 Codex home 或账户视图。现有单账户 `AuthManager` 继续处理当前 Agent 的凭据，多账户选择由 Agent 启动层完成。

### 16.3 其他 Provider

对于使用外部登录流程的 Provider：

- Token 通过 Provider 对应的外部流程获取。
- Codex 提供统一的 token 导入流程。
- token 可以由用户通过外部流程获取。
- 导入后加入本地 Provider Account Store。
- 导入结果写入 Team State Repository。
- 导入结果进入后续 Nix config generation。

接口形式例如：

```bash
codex account import \
  --provider kimi \
  --account team \
  --token-stdin
```

### 16.4 Token 刷新

Provider token 刷新后：

1. 更新对应账户的凭据。
2. 写入 Team State Repository 的配置状态。
3. 创建新的 credential revision。
4. Commit 并 push。
5. 生成新的配置 revision。
6. 必要时生成新的 Nix config generation。
7. Session 记录刷新前后的 revision。
8. 后续请求记录实际使用的 credential revision。

同一账户的多个 Agent 必须能够识别最新的凭据 revision。

## 17. 配置管理

### 17.1 配置范围

必须纳入 Team State Repository 和 `ConfigGeneration` 的配置包括：

- Codex `config.toml`。
- Codex profiles。
- Project configuration。
- Session overrides。
- Provider 和 Model 定义。
- Account 定义和明文凭据。
- Machine alias。
- Agent roles 和 Agent routing。
- Hooks、Rules 和 MCP servers。
- Skills 和 Memory 配置。
- Nix 配置。
- tmux runtime 配置。

### 17.2 复用配置分层

继续使用 Codex 现有配置分层系统，并支持仓库中的基础配置、团队配置、Machine 配置、Agent role 配置、Task 配置和 Session override。

有效配置必须能够描述：

```text
每个字段的来源
最终字段值
config commit
effective config digest
```

### 17.3 配置可恢复性

给定以下信息，必须能够恢复对应 Agent 启动时使用的完整配置：

```text
codex_source_repository
codex_source_commit
team_state_repository
team_state_commit
config_commit
flake.lock
machine_id
provider_id
account_id
model_id
```

## 18. Session 留存

### 18.1 全部 Session

必须保存所有 Session，包括：

- Root Session。
- 本地 Subagent Session。
- 远端 Subagent Session。
- 已完成 Session。
- 中断 Session。
- 失败 Session。
- 恢复后的 Session。
- 团队分析 Session。

所有 Session 都写入 Team State Repository，并永久保留完整历史。

### 18.2 Session 内容

每个 Root Session 必须保存以下信息。

#### 身份信息

- Root Session ID。
- AgentId。
- Parent 和 Child Agent 关系。
- Agent role 和 responsibility。
- Agent 使用的 `RoleDefinition` 和订阅事件。
- Agent status。
- Agent 的 input sources 和 output destinations。
- Agent Directory snapshot 和更新事件。
- MachineId 和 Machine alias。
- Repo。
- Base、Current 和 Final commit。
- Worktree path。
- Agent branch。

#### 运行环境

- Flake revision。
- `flake.lock` hash。
- Nix system。
- Derivation 和 store path。
- `ConfigGeneration`。
- Skills revision。
- Memory revision。

#### tmux 信息

- tmux session、window 和 pane。
- Gateway endpoint。
- 动态端口分配事件。
- Endpoint 变化。
- Attach 和 reconnect 事件。
- Agent start 和 stop 事件。

#### 对话和工具

- 用户输入。
- Agent 输出。
- Agent 间消息。
- `AgentMessage` 头部。
- 工具调用和工具结果。
- Shell 输入输出。
- tmux stdin 和 stdout。
- Rollout items。
- Compaction 事件。

#### Git 操作

- Worktree 创建。
- Branch 创建。
- 每个 checkpoint commit。
- 每次 push。
- Push remote。
- Hook 成功或失败。
- Final commit。
- Workspace diff 和输出。

#### Provider

- Provider。
- Account。
- Model。
- Credential revision。
- Provider 请求 ID。
- 使用量和可获得的成本信息。

#### Task 与构建

- `TaskSpec`。
- 是否编译和编译目标。
- Build log。
- 输出 derivation 和 store path。
- 最终产物。

### 18.3 tmux 原始记录

Session 同时保存完整 tmux 原始输入输出和解析后的结构化字段。

### 18.4 断线 Spool

远端断线时：

1. tmux 中的 Agent 继续运行。
2. Gateway 继续捕获 tmux 输出。
3. 输出写入所在机器的持久 spool。
4. 原 Root、Parent Agent 或其他接管该 Session 的 Agent 重新连接。
5. 缺失记录从 spool 同步。
6. 同步内容写入对应 Git Session ref。
7. Session 中记录断开、恢复和同步事件。

### 18.5 Context Compaction

模型上下文使用 Codex 现有 compaction。Git 中保存的完整原始 Session 始终保持原始内容。

## 19. Skills 和 Memory

### 19.1 团队统一维护

团队 Skills 和 Memory 必须保存在 Team State Repository。所有机器和 Agent 根据明确的 Team State Repository revision 加载相同版本。

### 19.2 保留现有格式

Codex 已有 Skill 目录、`SKILL.md`、Skill 加载器、Memory 数据结构和 Memory 引用机制是团队 Skill 与 Memory 的规范格式。

### 19.3 从 Session 整理知识

完整 Session 是团队分析输入。整理流程需要支持：

```text
原始 Session
→ observations
→ memory candidates
→ skill candidates
→ 团队确认
→ 正式 Memory/Skill
→ 新 Team State Repository revision
→ 新 ConfigGeneration
```

整理出的内容需要能够引用原始 Root Session ID、AgentId、Session commit、相关事件和 Repo commit。

### 19.4 分析 Agent

负责分析 Session、整理 Memory 和 Skill 的 Agent 也必须满足相同的 Nix、MachineId、tmux、worktree、branch、Provider、Account、Model、commit/push Hook、最终 Hook 和 Session 留存要求。

## 20. 核心数据绑定

每个生产 Agent 必须具有完整的 `AgentExecution`：

```text
AgentExecution
├── agent_id
├── parent_agent_id
├── root_session_id
├── role
├── responsibility
├── RoleBinding
├── TaskSpec
├── AgentDirectoryBinding
├── MachineContext
├── AgentWorkspace
├── NixGeneration
├── ConfigGeneration
├── AgentPlacement
├── TmuxEndpoint
├── InferenceBinding
└── GitSessionIdentity
```

生产 Agent 在所有必要绑定就绪后启动。

其中 `RoleBinding` 至少包含：

```text
RoleBinding
├── role_id
├── role_registry_revision
├── responsibility
├── input_sources[]
├── output_destinations[]
└── subscribed_events[]
```

其中 `GitSessionIdentity` 至少包含：

```text
GitSessionIdentity
├── team_state_repository
├── session_ref
└── session_commit
```

## 21. Subagent 完整生命周期

### 21.1 启动阶段

1. 创建或读取 Root Session。
2. 分配 AgentId。
3. 确定 Parent Agent、role 和 responsibility。
4. 读取该 role 的 `RoleDefinition`。
5. 读取当前 Agent Directory。
6. 明确 input sources、output destinations 和 subscribed events。
7. 解析 `TaskSpec`。
8. 确定 repo 和 base commit。
9. 选择目标 MachineId。
10. 确认目标机器当前 `hostid`。
11. 解析 flake 和 Nix build target。
12. 解析 `ConfigGeneration`。
13. 确定 Provider、Account、Model 和 credential revision。
14. 创建独立 branch。
15. 在目标机器 `/tmp` 下创建独立 worktree。
16. 创建或选择对应 tmux session。
17. 创建 Subagent window 和 pane。
18. 启动 tmux gateway。
19. 使用 `bind(port = 0)` 获取动态端口。
20. 将 endpoint 和 AgentDescriptor 发布到 Agent Directory。
21. 在指定 Nix environment 中启动 Agent。
22. 通过 Subagent 的 tmux stdin 发送 `AgentBootstrapContext`。
23. 将全部绑定写入 Session。

### 21.2 工作阶段

1. Agent 在自己的 worktree 中工作。
2. 所有工具和 shell 命令在绑定的 Nix 环境中执行。
3. 每次 workspace 修改后运行 checkpoint Hook。
4. Commit 并 push 后更新 current commit。
5. Agent 可以按需查询当前完整 Agent Directory。
6. Agent 接收与自己的 role、Task、路由关系或订阅事件相关的 Directory 更新。
7. Agent 发送消息前按照 `RoleDefinition` 和 Agent Directory 解析具体目的地。
8. Agent 间消息带完整来源、目的地、repo 和 commit。
9. 消息通过 tmux stdout、socket 和目标 tmux stdin 定向传递。
10. Agent 状态、职责、routing、repo、commit 和 endpoint 变化形成 Directory 更新。
11. Directory 更新定向通知相关 Agent。
12. 工具、消息、构建、Directory 更新和 Git 操作写入 Session。

### 21.3 结束阶段

1. 将 Agent Directory 状态更新为 finalizing。
2. 执行最终 Git Hook。
3. Commit 所有剩余修改。
4. Push Agent branch。
5. 确认最终 `HEAD` 已推送。
6. 记录 final commit。
7. 根据 Agent Directory 确定最终结果的具体接收 Agent。
8. 发送带完整来源、目的地和 final commit 的最终 `AgentMessage`。
9. 将最终结果写入 Session。
10. 将 Agent Directory 状态更新为 completed。
11. 关闭对应 tmux window 和 pane。

## 22. 需要新增或扩展的组件

当前确认需要补充的能力包括：

```text
NixTaskResolver
  解析 TaskSpec、flake、build target 和 derivation

MachineCatalog
  使用 hostid 管理机器身份，并从 Git 读取 alias

RoleRegistry
  定义 role 的职责、输入、输出、路由候选和状态事件订阅

TmuxAgentRuntime
  管理 Agent 与 tmux session、window、pane 的生命周期

TmuxGateway
  将本地 tmux Unix socket 暴露为动态明文 TCP socket

AgentDirectory
  保存 Session 内 Agent 的身份、role、职责、状态、路由、placement 和动态 endpoint

AgentMessageTransport
  按 role 和 AgentId 通过 tmux stdout、socket、stdin 定向传递消息

GitAccountStore
  管理多个 Provider 和多个账户

ConfigGeneration
  固定代码、配置、凭据、Skills、Memory 和 Nix revision

AgentWorkspaceManager
  创建和恢复 /tmp 下的独立 worktree 和 branch

GitCheckpointHook
  每次修改后 commit 和 push

GitFinalizationHook
  Agent 结束前完成最终 commit 和 push

GitSessionAdapter
  将现有 ThreadStore、Rollout 和完整 tmux 记录写入 Team State Repository refs
```

这些组件连接现有 Codex 模块，并复用现有 Agent、Session、Hook、Skill 和 Memory 系统。
