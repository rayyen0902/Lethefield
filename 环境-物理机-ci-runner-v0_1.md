# 环境验收：物理机 CI/演练机 v0.1

验收日期：2026-08-13（UTC）/ 2026-08-14（CST）
验收人：工程会话（Kimi Code CLI）
验收方式：Mac（macbook-pro-1）经 Tailscale SSH 直连目标机逐项执行

## 1. 机器硬件配置

| 项 | 实测值 |
|---|---|
| CPU | Intel Core i9-14900K，1 路 24 核 32 线程（`nproc` = 32） |
| 内存 | 62 GiB（available 61 GiB）+ 8 GiB swap |
| 系统盘 | `/dev/nvme0n1p6`，195 G，已用 14 G（8%），可用 172 G |
| GPU | NVIDIA RTX A4000 15 GB（15352 MiB），驱动 595.84，CUDA Version 13.2（驱动上报） |
| 架构 | x86_64 |

GPU 现状：驱动已就绪（`nvidia-smi` 正常，空闲 7 W / 34 °C）。1.0 阶段不使用 GPU，未安装 CUDA toolkit，2.0/3.0 阶段再配。

## 2. Tailscale 网络位置

- 主机名：`ubunturay`，Tailscale IPv4：`100.92.236.89`（账号 rayyen0902@）
- 验收期间在线，与 Mac（macbook-pro-1, 100.119.34.126）为 **direct 直连**（123.139.101.18:10490），未走 DERP 中继
- SSH 入口：`ssh ray@ubunturay`（本机用户 `caopinggege` 在目标机不存在，必须用 `ray@`；host key 已加入 known_hosts）

## 3. 验收结果

### 3.1 系统与 Docker

- OS：Ubuntu 24.04.4 LTS (noble)，内核 6.8.0-137-generic，x86_64
- Docker：Server/Client **29.7.2**，`docker-ce 5:29.7.2-1~ubuntu.24.04~noble`，**官方 docker-ce apt 源**（经清华 tuna 镜像 `mirrors.tuna.tsinghua.edu.cn/docker-ce`），**非 snap**（`snap list` 无 docker）
- Compose：**插件版 v5.4.0**（`docker compose version`），无独立 `docker-compose`

原始输出摘录：

```
ii  docker-ce    5:29.7.2-1~ubuntu.24.04~noble   amd64   Docker: the open-source application container engine
ii  containerd.io 2.3.3-1~ubuntu.24.04~noble      amd64   An open and reliable container runtime
Docker Compose version v5.4.0
```

### 3.2 时钟同步（红线 6 前置）

`timedatectl status`：

```
System clock synchronized: yes
NTP service: active          （systemd-timesyncd active，chrony inactive——timesyncd 已满足要求，未装 chrony）
Time zone: Asia/Shanghai (CST, +0800)
```

通过。附注：存在 `RTC in local TZ: yes` 警告（双系统遗留配置）。不影响 NTP 同步语义，未改动——改 `set-local-rtc 0` 会影响 Windows 双启动时钟显示，留待机主决定（见 §5 缺口）。

### 3.3 组网

见 §2。`tailscale status` 在线、IP 固定 100.92.236.89、与 Mac 直连。

### 3.4 资源

见 §1。余量充足（内存 61 GiB 空闲、磁盘 172 G 可用），远超 Mac 侧 colima VM（10 GiB）的踩坑水位。

### 3.5 实战验证：全量 CI

部署方式：Mac 工作副本 `rsync -az --delete` 至 `ray@ubunturay:~/Lethefield/`（排除 `.venv/`、`.pytest_cache/`、`.ruff_cache/`、`__pycache__/`、`.DS_Store`、`var/`、`.env`）；`.env` 单独 scp 同步，md5 双侧一致（`893b5aa54cff709bf90e406c1a84199f`），权限 600。

前置补装：`uv`（官方 installer，用户级 `~/.local/bin`，uv 0.12.3）；git 2.43.0 / make 4.3 / rsync 系统自带；用户 `ray` 已在 `docker` 组。

执行：`make reset && bash scripts/ci.sh`（uv sync → ruff → 单测 → M8/M13 静态巡检 → 起栈 → 集成测试）。

**结果：全绿，CI_EXIT=0，总耗时 8 分 35 秒**（00:46:00 → 00:54:35 CST，Docker 镜像已为首轮拉取缓存）。

```
==> uv sync / ruff check & format      All checks passed!
==> unit tests                         505 passed, 26 warnings in 3.02s
==> M8 space model check               通过
==> M13 redline 1 static scan          通过
==> M13 redlines summary check         通过
==> integration tests                  122 passed, 1 skipped, 364 warnings in 422.21s
==> CI OK
```

（1 skipped = M10 跨 Cell 迁移演练，cell2 profile 默认不进 CI，符合预期。）

**首跑失败与修复（已复验）**：首次执行在 `make reset` 的 `wait_for_stack.sh` 阶段失败——uv 直连 pypi.org 下载 `pulsar-client==3.13.0` 时 connection reset（大陆网络直连 PyPI 的典型症状）。处置：写用户级 `~/.config/uv/uv.toml`，默认索引切清华镜像（`pypi.tuna.tsinghua.edu.cn/simple`），不动仓库代码；复验零下载错误、全量通过。完整日志在目标机 `~/ci_full_run2.log`（首跑失败日志 `~/ci_full_run.log`）。

## 4. CI 耗时对比

| 环境 | 范围 | 耗时 |
|---|---|---|
| **物理机 ubunturay（本次，镜像已缓存）** | make reset + ci.sh 全量 | **8 分 35 秒** |
| GitHub Actions：M17（2026-08-13） | 干净环境从零起栈 | ~11 分 17 秒 |
| GitHub Actions：CI 修复（2026-08-11） | 同上 | ~6 分 41 秒 |
| GitHub Actions：README M11（2026-08-07） | 同上 | ~6 分 18 秒 |

物理机 8m35s 处于 GitHub Actions 基线区间内，其中集成测试 422s（~7 分钟）为绝对大头，单测仅 3s——瓶颈在起栈与集成测试的存储等待，不在算力。首跑（冷镜像拉取全量 8+ 组件 + PyPI 故障）耗时 25m44s 且未跑通，不作基线。Mac 侧无全量 CI 计时记录（开发期用 colima VM，10 GiB 内存曾 OOM 杀 JanusGraph），物理机 62 GiB 内存无此风险。

## 5. 发现的缺口与处理建议

1. **uv 直连 PyPI 被重置**（已修复并复验）：用户级 `~/.config/uv/uv.toml` 切清华镜像后零下载错误。后续在该机跑 CI/演练直接可用；若换机器需带上此配置。
2. **uv 缺失**（已处理）：按官方 installer 装至用户级 `~/.local/bin`（uv 0.12.3），CI 通过 `PATH` 前缀生效；建议机主把 `source $HOME/.local/bin/env` 写入 `~/.bashrc`（installer 已提示）。
3. **RTC in local TZ 警告**（未处理，待机主决定）：不影响 NTP 同步；若机器不双启动 Windows，建议 `timedatectl set-local-rtc 0` 消除警告。
4. **SSH 用户名**：目标机无 `caopinggege` 用户，统一用 `ray@ubunturay`；建议 Mac 侧 `~/.ssh/config` 加 `Host ubunturay / User ray` 固化。
5. **建议**：仓库同步用 rsync 工作副本的方式已验证可行（含 .git，排除 .venv/var 等派生物）；后续如需常态化，可把 §3.5 的 rsync 命令固化成脚本。
2. **RTC in local TZ 警告**（未处理，待机主决定）：不影响 NTP 同步；若机器不双启动 Windows，建议 `timedatectl set-local-rtc 0` 消除警告。
3. **SSH 用户名**：目标机无 `caopinggege` 用户，统一用 `ray@ubunturay`；建议 Mac 侧 `~/.ssh/config` 加 `Host ubunturay / User ray` 固化。

## 6. 移机复验记录

- 2026-08-14：物理机移机（更换物理位置/网络）。移机前 `make down` 干净停栈（卷保留）+ Mac 侧 `~/.ssh/config` 补 `Host ubunturay / User ray`（§5 缺口 4 落地）；复电后 Tailscale IP 不变（100.92.236.89 直连），`make up` 10 容器齐全，按验收同口径 `make reset && bash scripts/ci.sh` 全量复验**全绿**（集成 122 passed / 1 skipped，426.81s ≈ 基线 422.21s），环境无损。注：非交互 ssh 的 PATH 不带 `~/.local/bin`，远程跑 make/CI 需 `export PATH="$HOME/.local/bin:$PATH"` 前缀。
