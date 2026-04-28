# WebDAV Backup

一个可用 Docker 部署的文件夹备份服务。网页端可以配置 WebDAV、选择容器内挂载目录、设置备份周期和保留版本数。每次备份会把目标目录打成 `.tar.gz` 并上传到 WebDAV，超过保留数量后删除最旧版本。

Docker 镜像：

```text
vectorzhao/vaultpack:1.0.0
vectorzhao/vaultpack:latest
```

## 功能

- WebDAV 地址、账号、密码和远端目录配置
- 从容器挂载根目录中选择要备份的子目录
- 使用 cron 表达式设置备份时间，按容器 `TZ` 时区执行
- 按任务设置保留版本数，例如只保留最近 5 个包
- 管理员网页登录
- 可选 TOTP 二次验证，兼容 Google Authenticator、1Password、Microsoft Authenticator 等
- 支持手动立即备份
- SQLite 保存配置和运行记录

## 快速启动

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

如果配置了 `ADMIN_PASSWORD`，首次启动会自动创建管理员账号；否则首次打开会要求创建管理员账号。之后进入 WebDAV 页面保存连接信息，再创建备份任务。

## 挂载目录说明

容器只允许备份 `BACKUP_SOURCE_ROOT` 下的目录，默认是：

```text
/backup-source
```

如果宿主机挂载为：

```yaml
- /srv/data:/backup-source:ro
```

网页里选择的 `photos` 实际对应宿主机：

```text
/srv/data/photos
```

## 数据持久化

`/data` 中保存：

- `backup.db`：用户、WebDAV 配置、任务、运行记录

备份压缩包会先临时生成在 `/tmp/backup-work`，上传成功或失败后都会清理本地临时文件。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `BACKUP_SECRET_KEY` | `dev-change-me` | Flask 会话密钥，生产环境必须修改 |
| `ADMIN_USERNAME` | `admin` | 首次启动时自动创建的管理员用户名，仅在数据库还没有用户且设置了 `ADMIN_PASSWORD` 时生效 |
| `ADMIN_PASSWORD` | 未设置 | 首次启动时自动创建的管理员密码，至少 8 位；留空则走网页初始化 |
| `BACKUP_DATA_DIR` | `/data` | 配置和数据库目录 |
| `BACKUP_SOURCE_ROOT` | `/backup-source` | 允许在网页中选择的备份根目录 |
| `BACKUP_WORK_DIR` | `/tmp/backup-work` | 压缩临时目录 |
| `TZ` | `Asia/Shanghai` | 容器时区，也是 Web 中 cron 表达式的执行时区 |
| `BACKUP_TIMEZONE` | 未设置 | 可选，优先级高于 `TZ`，用于单独指定 Web cron 执行时区 |

## Docker Hub 发布

GitHub Actions 会在 `main` 分支推送时构建并推送多架构镜像：

- `linux/amd64`
- `linux/arm64`

需要在 GitHub 仓库 Secrets 中配置：

| Secret | 说明 |
| --- | --- |
| `DOCKERHUB_USERNAME` | Docker Hub 用户名 |
| `DOCKERHUB_TOKEN` | Docker Hub Access Token 或密码 |

推送的标签为 `vectorzhao/vaultpack:1.0.0` 和 `vectorzhao/vaultpack:latest`。

## 备份命名和保留策略

上传文件名格式：

```text
job-任务ID-任务名-YYYYMMDDTHHMMSSZ.tar.gz
```

保留策略按任务 ID 匹配同一任务的备份文件，按文件名时间顺序删除旧文件，只保留最新的 `retention_count` 个。
