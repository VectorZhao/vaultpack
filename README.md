# vaultpack

一个可用 Docker 部署的多节点文件夹备份服务。vaultpack 支持中心面板统一管理存储目的地、备份任务、节点和运行记录；每台需要备份的 VPS 可以部署轻量 Agent，Agent 主动连接面板领取任务，在本机压缩目录并直接上传到 WebDAV 等存储目的地。

Docker 镜像：

```text
vectorzhao/vaultpack:2.1.3
vectorzhao/vaultpack:latest
```

## 功能

- 中心面板 + 节点 Agent 架构，适合统一管理多台 VPS
- Agent 主动通过 HTTPS 轮询面板，无需在被备份 VPS 暴露端口
- 节点接入令牌注册，注册后使用长期节点 token
- 节点在线状态、版本、主机名、最后心跳和挂载根目录展示
- WebDAV 地址、账号、密码和远端目录配置
- 新建任务时选择节点，再浏览该节点挂载根目录中的子目录
- 使用 cron 表达式设置备份时间，按容器 `TZ` 时区执行
- 按任务设置保留版本数，例如只保留最近 5 个包
- 管理员网页登录
- 可选 TOTP 二次验证，兼容 Google Authenticator、1Password、Microsoft Authenticator 等
- PWA 支持，可在手机浏览器中添加到主屏幕并以独立应用窗口使用
- 支持手动立即备份
- SQLite 保存配置和运行记录

## 快速启动：单机模式

先编辑 `docker-compose.yml`：

```yaml
volumes:
  - ./data:/data
  - /path/to/your/server/folder:/backup-source:ro
```

把 `/path/to/your/server/folder` 换成宿主机上需要暴露给容器选择的目录。然后修改 `BACKUP_SECRET_KEY`、`ADMIN_USERNAME` 和 `ADMIN_PASSWORD`。`TZ` 用来指定 Web 中 cron 表达式的执行时区，不设置时默认使用 `Asia/Shanghai`。

启动：

```bash
docker compose up -d
```

访问：

```text
http://服务器IP:18080
```

如果配置了 `ADMIN_PASSWORD`，首次启动会自动创建管理员账号；否则首次打开会要求创建管理员账号。单机模式会自动创建一个 `local` 本机节点，之后进入“存储目标”添加 WebDAV，再进入“备份任务”创建任务即可。

## 多节点部署

面板部署在一台 VPS 上：

```yaml
services:
  vaultpack:
    image: vectorzhao/vaultpack:latest
    container_name: vaultpack
    restart: unless-stopped
    ports:
      - "18080:8080"
    environment:
      VAULTPACK_ROLE: "panel"
      TZ: "Asia/Shanghai"
      BACKUP_SECRET_KEY: "change-me"
      ADMIN_USERNAME: "admin"
      ADMIN_PASSWORD: "change-me-strong-password"
    volumes:
      - ./data:/data
```

在面板的“节点”页面复制 Agent 部署命令，到每台需要备份的 VPS 执行。Agent 至少需要配置：

```bash
docker run -d --name vaultpack-agent --restart unless-stopped \
  -e VAULTPACK_ROLE=agent \
  -e PANEL_URL=https://你的面板地址 \
  -e AGENT_ENROLL_TOKEN=面板生成的接入令牌 \
  -e AGENT_NAME=$(hostname) \
  -v /path/to/backup:/backup-source:ro \
  -v vaultpack-agent-data:/data \
  vectorzhao/vaultpack:latest
```

Agent 首次注册成功后会把长期 token 保存到 `/data/agent-token`。生产环境建议持久化 `/data`，或者把日志中输出的长期 token 写入 `AGENT_TOKEN`，后续就不再依赖接入令牌。

多节点任务执行流程：

1. 面板按 cron 调度任务并创建运行记录。
2. Agent 主动轮询面板，领取 `run_backup` 命令。
3. Agent 在本机扫描、压缩并直接上传到存储目的地。
4. Agent 持续上报进度、最终状态、文件名和大小。

## 挂载目录说明

本机模式或 Agent 都只允许备份各自容器内 `BACKUP_SOURCE_ROOT` 下的目录，默认是：

```text
/backup-source
```

如果宿主机挂载为：

```yaml
- /srv/data:/backup-source:ro
```

网页里在对应节点下选择的 `photos` 实际对应该节点宿主机：

```text
/srv/data/photos
```

## 数据持久化

面板 `/data` 中保存：

- `backup.db`：用户、存储目标、节点、任务、运行记录

Agent `/data` 中保存：

- `agent-token`：节点注册后的长期 token

备份压缩包会先临时生成在执行节点的 `/tmp/backup-work`，上传成功或失败后都会清理本地临时文件。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `VAULTPACK_ROLE` | `panel` | 运行角色，面板使用 `panel`，节点使用 `agent` |
| `BACKUP_SECRET_KEY` | `dev-change-me` | Flask 会话密钥，生产环境必须修改 |
| `ADMIN_USERNAME` | `admin` | 首次启动时自动创建的管理员用户名，仅在数据库还没有用户且设置了 `ADMIN_PASSWORD` 时生效 |
| `ADMIN_PASSWORD` | 未设置 | 首次启动时自动创建的管理员密码，至少 8 位；留空则走网页初始化 |
| `BACKUP_DATA_DIR` | `/data` | 配置和数据库目录 |
| `BACKUP_SOURCE_ROOT` | `/backup-source` | 允许在网页中选择的备份根目录 |
| `BACKUP_WORK_DIR` | `/tmp/backup-work` | 压缩临时目录 |
| `TZ` | `Asia/Shanghai` | 容器时区，也是 Web 中 cron 表达式的执行时区 |
| `BACKUP_TIMEZONE` | 未设置 | 可选，优先级高于 `TZ`，用于单独指定 Web cron 执行时区 |
| `PANEL_URL` | 未设置 | Agent 连接的面板地址，例如 `https://backup.example.com` |
| `AGENT_ENROLL_TOKEN` | 未设置 | Agent 首次注册使用的接入令牌，由面板“节点”页面生成 |
| `AGENT_TOKEN` | 未设置 | Agent 注册后的长期 token；设置后会跳过接入令牌注册 |
| `AGENT_NAME` | 主机名 | Agent 在面板中显示的节点名称 |
| `AGENT_POLL_INTERVAL` | `10` | Agent 轮询面板的间隔秒数，最小为 2 |

## PWA 安装

vaultpack 提供 Web App Manifest 和 Service Worker，可在手机浏览器中添加到主屏幕并以独立应用窗口使用。生产环境请通过 HTTPS 访问面板；除 `localhost` 外，主流手机浏览器通常会拒绝在普通 HTTP 页面注册 Service Worker。

## Docker Hub 发布

GitHub Actions 会在发布 GitHub Release 时构建并推送多架构镜像。Release tag 使用 `v2.1.3` 这类格式时，Docker 镜像会自动去掉开头的 `v`，发布为 `2.1.3`。

- `linux/amd64`
- `linux/arm64`

需要在 GitHub 仓库 Secrets 中配置：

| Secret | 说明 |
| --- | --- |
| `DOCKERHUB_USERNAME` | Docker Hub 用户名 |
| `DOCKERHUB_TOKEN` | Docker Hub Access Token 或密码 |

例如发布 `v2.1.3` 时，推送的标签为 `vectorzhao/vaultpack:2.1.3` 和 `vectorzhao/vaultpack:latest`。

## 备份命名和保留策略

上传文件名格式：

```text
节点名-j任务ID-YYYYMMDD-HHmm.tar.gz
```

例如：

```text
local-j2-20260430-0200.tar.gz
sg-arm01-j5-20260430-0215.tar.gz
```

保留策略按任务 ID 匹配同一任务的备份文件，按文件名时间顺序删除旧文件，只保留最新的 `retention_count` 个。升级前生成的 `job-任务ID-任务名-YYYYMMDDTHHMMSSZ.tar.gz` 旧格式文件仍会参与保留策略清理。

# 致谢
- [Codex](https://openai.com/zh-Hans-CN/codex/)
- [Linux DO](https://linux.do/)
