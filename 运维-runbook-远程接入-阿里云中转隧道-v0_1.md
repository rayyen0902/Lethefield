# 运维 runbook：远程接入（阿里云中转 SSH 隧道）v0.1

- 日期：2026-08-19
- 场景：栈常驻办公室物理机 ubunturay，用户在家/外地用 Mac 工作。Tailscale 跨网络不稳定，
  改用阿里云中转通道（`ssh -p 6000 ray@39.106.89.255`）+ SSH 本地端口转发。
- 原理：把 ubunturay 上栈的全部端口原样转发到 Mac localhost 同端口；客户端工厂默认值
  （`libs/clients/factories.py`）就是 localhost + 同端口，**MCP server / IS CLI 零环境变量改动**。
  单节点形态下 Cassandra/Pulsar 的地址再发现不会穿帮（本地 CI 走 docker-proxy 同端口已实证）。

## 0. 前置

- 本 runbook 已于 2026-08-19 全链实测：8 端口转发全绿（ES 返回 ubunturay 真实集群
  `lethefield-graph` 信息），`lethefield_is account list` 经隧道真实执行成功。
- Mac 本地栈必须**停掉**（端口冲突）：`cd ~/Desktop/Lethefield && make down`
  （colima 栈和隧道占用同一批 localhost 端口，只能二选一）。
- Mac 能免密 SSH 到中转通道（key 已配好，见《环境-物理机-ci-runner-v0_1.md》§2）。
- ubunturay 上栈在跑：`ssh -p 6000 ray@39.106.89.255 'docker ps'` 应见 10 个容器。

## 1. 建立隧道（每天开工一条命令）

```bash
ssh -N -f \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
  -L 9042:localhost:9042 \
  -L 9043:localhost:9043 \
  -L 9200:localhost:9200 \
  -L 9201:localhost:9201 \
  -L 8182:localhost:8182 \
  -L 6650:localhost:6650 \
  -L 6379:localhost:6379 \
  -L 5432:localhost:5432 \
  -p 6000 ray@39.106.89.255
```

参数说明：`-N` 不开 shell 纯转发；`-f` 后台；`ServerAliveInterval=30` 每 30 秒心跳
保活（家用 NAT 断空闲连接的对策）；`ExitOnForwardFailure` 端口被占时直接报错
（提示你本地栈没停干净）。

**建议做成 alias**（`~/.zshrc` 加一行）：

```bash
alias lethe-tunnel='ssh -N -f -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes -L 9042:localhost:9042 -L 9043:localhost:9043 -L 9200:localhost:9200 -L 9201:localhost:9201 -L 8182:localhost:8182 -L 6650:localhost:6650 -L 6379:localhost:6379 -L 5432:localhost:5432 -p 6000 ray@39.106.89.255'
```

以后开工 `lethe-tunnel`，收工 `pkill -f "ssh -N -f.*6000"`。

断线自愈（可选进阶）：装 autossh（`brew install autossh`），把上面命令的 `ssh`
换成 `autossh -M 0`，断线自动重连。

## 2. 验证隧道（10 秒）

```bash
nc -z localhost 9042 && echo cassandra-cell OK
nc -z localhost 9043 && echo cassandra-ex OK
curl -s localhost:9200 | head -3        # ES 返回 JSON 即 OK
nc -z localhost 8182 && echo janusgraph OK
nc -z localhost 6650 && echo pulsar OK
nc -z localhost 6379 && echo redis OK
nc -z localhost 5432 && echo postgres OK
```

全绿后继续。任一失败：先确认 Mac 本地栈已 `make down`，再确认 ubunturay 栈在跑。

## 3. 首次配置（账号 → 空间 → 凭证，只做一次）

以下命令在 Mac 仓库目录执行，全部经隧道作用于 ubunturay：

```bash
cd ~/Desktop/Lethefield

# 3.1 建账号（记下 account_id，也可直接指定短名如 ray）
uv run python -m lethefield_is account create

# 3.2 每个项目建一个 space（走真实开通流水线：EX → Pulsar → RMS → 注册）
uv run python -m lethefield_is space create lethefield_dev --account <account_id>
uv run python -m lethefield_is space create ecomagentos --account <account_id>
# space_id 规则：[a-z0-9_] ≤40 字符，一个项目一个，建好后不可改名

# 3.3 每个 space 签一个凭证（--space 可重复传多个，但建议一项目一证，便于单独吊销）
uv run python -m lethefield_is credential issue \
  --account <account_id> --actor-id kimi-cli \
  --space lethefield_dev \
  --scopes record,retrieve,reinforce,flag_conflict
# 输出 JWT，妥善保存（这是密钥，别进 git、别明文贴聊天）
uv run python -m lethefield_is credential issue \
  --account <account_id> --actor-id kimi-cli \
  --space ecomagentos \
  --scopes record,retrieve,reinforce,flag_conflict
```

凭证默认 TTL 24h（`LETHEFIELD_IS_TOKEN_TTL_SECONDS` 可调，如 604800=一周）；
过期/泄露就 `credential revoke --jti <jti>` 后重签（重签发即刷新）。

## 4. 项目绑定（每个项目目录配一次）

在项目根目录放 MCP 配置（Claude Code 为 `.mcp.json`，其他客户端对应项目级配置）：

```json
{
  "mcpServers": {
    "lethefield": {
      "command": "uv",
      "args": ["run", "--project", "/Users/caopinggege/Desktop/Lethefield",
               "python", "-m", "lethefield_api.mcp_server"],
      "env": {
        "LETHEFIELD_MCP_TOKEN": "<该项目 space 的 JWT>"
      }
    }
  }
}
```

要点：`--project` 指向仓库（MCP server 代码在仓库里，uv 需要找到 workspace）；
token 决定该项目的 agent 读写哪个 space。**在哪个项目目录开 agent，就自动读写
哪个项目的记忆**，切换项目 = 切换空间，零手动操作。

## 5. 日常工作流

1. `lethe-tunnel`（断网/合盖后重连就再跑一次）；
2. 验证（§2 任选两条）；
3. 在项目目录开 agent 干活；
4. 收工 `pkill -f "ssh -N -f.*6000"`（不杀也行，隧道空闲无成本，但 token 在配置里，
   人走锁屏）。

## 6. 已知边界与注意

- **被动上传 hook 与 MCP 说明书未落地**：当前记忆写入靠人工提示 agent 调
  `memory_record`（建议开工说一句"重要决策与进展写入记忆"）。两项已列入种子期计划。
- 隧道只覆盖栈端口；Grafana(3000)/Prometheus(9090) 如需远程看监控，追加
  `-L 3000:localhost:3000 -L 9090:localhost:9090`。
- 隧道承载全部存储流量，家用上行带宽影响 record 延迟（EX ack 同步路径）；
  冒烟级使用无感，批量回填数据时建议在 ubunturay 本地执行。
- 阿里云中转是公网暴露面：ubunturay sshd 必须 key-only（《环境-物理机-ci-runner-v0_1.md》
  §2 安全待办，sudo 窗口核实 `PasswordAuthentication no` + fail2ban）。
- token 即钥匙：泄露立即 `credential revoke --jti`；定期轮换。
