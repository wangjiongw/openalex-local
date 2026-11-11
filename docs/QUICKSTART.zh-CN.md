# Linux 原生部署指南

[English](QUICKSTART.md) · [文档导航](INDEX.zh-CN.md)

需要 Python 3.10+、glibc Linux 和足够的持久磁盘。
可选文本索引还需 `dpkg-deb`（通常由 `dpkg` 提供）解包 Manticore。
AWS CLI 仅用于用户自行获取数据，不是后续处理依赖。
数据库无需 root 系统安装。源码和 workspace 请使用不同目录。

```bash
git clone https://github.com/wangjiongw/openalex-local.git
cd openalex-local
git checkout v0.3.1
python3 -m venv .venv
.venv/bin/pip install .
export PATH="$PWD/.venv/bin:$PATH"
export OPENALEX_HOME="$HOME/openalex-workspace"
oa init
```

也可以从 [GitHub 稳定版](https://github.com/wangjiongw/openalex-local/releases/tag/v0.3.1)
下载 wheel 和 `SHA256SUMS.txt`，验证 SHA-256 后，在虚拟环境运行
`python -m pip install /path/to/openalex_local_api-0.3.1-py3-none-any.whl`。
无需依赖 PyPI 发布。wheel 包含网页和规则，原始快照与引擎二进制另行准备。

`--workspace /absolute/path` 优先于 `OPENALEX_HOME`；均未指定时使用当前目录。
私有凭据生成在权限为 0600 的 `config/local.json`，不要加入 Git。

## 准备本地快照

按照[快照指引](SNAPSHOT.zh-CN.md)自行下载需要的 OpenAlex 版本，本项目不负责下载。
保留清单和相对路径，然后指定数据已有的位置：

```bash
oa init --snapshot /absolute/path/to/openalex-snapshot
oa inventory
oa snapshot verify --deep
```

这只更新配置路径，不复制快照；支持只读源挂载。
可读取旧 `data/<entity>/manifest` 和新 `data/jsonl/manifest.json`，尚不支持 Parquet 导入。
部分快照需提供描述实际文件的清单，发布后仍会标记为不完整。

```bash
oa engines ensure  # ClickHouse only; no text engine required
# Or use matching pinned archives in a local cache:
oa engines ensure --offline --cache /path/to/engine-cache
```

引擎版本与哈希固定，安装在 workspace 内。默认 ClickHouse LTS 支持 Linux amd64／arm64。
较旧 ARM CPU 可能需要 `--profile armv8-compat`；如果上游兼容包 URL 变化，离线缓存需提供相应固定版本压缩包。
切换 profile 不会覆盖已安装引擎，应使用另一个 workspace。

## 默认路径：元数据与引用服务

启动服务前编辑本地配置。默认资源预算为 16 GiB，ClickHouse 上限 8 GiB，最多 8 CPU、4 个导入 worker，API 端口 8088。
ClickHouse HTTP／TCP 默认为 18123／19000；可选 Manticore HTTP 为 19308。
如有冲突，配置 `port`、`ch_url`、`ch_tcp_port` 和使用时的 `manticore_url`。
完整快照需要大量持久磁盘，可能还需提高预算。增加 worker 前先测存储吞吐。

```bash
oa pipeline plan --generation local_metadata
oa pipeline run --generation local_metadata
# The default run imports and audits metadata; publishing is explicit:
oa pipeline resume --generation local_metadata --until published
oa pipeline status --generation local_metadata
```

默认 `metadata` profile 读取提供的全部实体，保留完整源记录，构建 DOI／ID 映射与正反向引用关系。
此路径不进行学科分类，也不创建文本索引。
发布会启动提供元数据访问的 API；标题／名称全文搜索需另行建立可选索引。
因此 `/readyz` 可以显示元数据部署已就绪，同时 `/status` 明确显示 `search_ready: false`。

恢复会复用已完成阶段，拒绝变化的输入或 profile，不强制等待 24 小时。
新快照需要新的 generation。已有地学计划恢复时必须传 `--profile earth_sciences`，不能原地静默更换 profile。

## 可选示例：地球科学文本检索

地学是内置的子集示例，不是项目必需范围。显式安装可选引擎：

```bash
oa engines ensure --search
```

从快照建立新的 generation：

```bash
oa pipeline plan --generation earth_example --profile earth_sciences
oa pipeline run --generation earth_example --profile earth_sciences
oa pipeline resume --generation earth_example --profile earth_sciences --until published
```

该示例增加地学分类、版本化集合导出、Manticore 原生索引、功能／负载／恢复验证及显式发布。
默认停止在 `validated`。完整快照按既有 30 分钟负载策略验收；部分／演示数据使用注明范围的短时并发检查，不构成全量吞吐证明。

如要复用已导入的 `local_metadata`，直接调用现有模块，无需再次导入或修改已保存的 profile：

```bash
oa search-catalog local_metadata
oa earth-export local_metadata
oa earth-export-verify local_metadata
oa search-build local_metadata earth_example
oa search-validate local_metadata earth_example
oa search-publish local_metadata earth_example
```

当前只提供地学文本 profile；元数据服务与其选择独立。本版尚无任意学科文本索引 profile。

也可不下载真实论文，用合成数据试运行：

```bash
python -m openalex_api.demo "$OPENALEX_HOME/snapshots/synthetic"
oa init --snapshot "$OPENALEX_HOME/snapshots/synthetic"
# Run the default metadata pipeline, or opt into the Earth example above.
```

## 本机或跨 Pod 查询

API 监听配置中的 `host`（默认 `0.0.0.0`）和 `port`。
确认 Pod IP 可达且网络策略放行端口。`0.0.0.0` 是监听地址，不是客户端访问目标。
从私有配置取用密钥，不要粘贴到公共报告或仓库。

```bash
export OA_URL=http://POD_IP:8088
# Supply OA_API_KEY securely from your local config.
curl "$OA_URL/readyz"
curl -H "X-API-Key: $OA_API_KEY" --get "$OA_URL/v1/works/lookup" \
  --data-urlencode 'identifier=10.99999/demo.2'
# Use a DOI present in your input; demo.2 exists only in the synthetic fixture.
```

网页入口为 `http://POD_IP:8088/`，输入相同密钥；`/docs` 列出 API 路由。
默认不启用每密钥 QPS 限制或调用方并发拒绝，但 worker、数据库超时等仍限制实际容量。
发布报告不自动证明跨 Pod 已测试。对于已发布文本索引，从另一 Pod 执行以下命令；
该客户端需能读取同一 workspace，或持有安全复制的客户端配置与样例：

```bash
oa verify --generation earth_example --base-url "$OA_URL" \
  --remote-client --output "$OPENALEX_HOME/reports/remote-verification"
```

服务管理：`oa start clickhouse`、`oa start api`、`oa status`、`oa stop all`、`oa logs api`。
重启带已发布文本索引的部署时还需 `oa start manticore`，纯元数据部署不需要。
单独验证恢复可执行 `oa restore --generation earth_example --output /path/to/new-report`。
它启动临时空 Manticore，检查快照／回执和查询结果，并停止自己创建的进程；
只读复用既有元数据服务，不是 ClickHouse 灾备测试。审核报告后可能需要手动清理临时恢复文件。

应保留原始快照、workspace 配置／密钥、ClickHouse 持久数据、generation 清单／检查点、
文献集合、索引快照和父分段。兼容 Pod 复用已建索引无需重新构建。
迁移前停止原 writer，不能让两个 writer 打开同一 ClickHouse 数据目录。
主机所有权标记会拒绝自动接管；确认旧进程全部停止后，再显式释放 `var/run/workspace-owner.json`。
不要删除仍被已发布索引引用的父分段，并保留独立备份。

## 可选：内部免密部署

默认开启认证。从 v0.3.1 起，可在现有 workspace 的 `config/local.json` 中设置
`"auth_required": false`，关闭 API key 检查。保留其他设置和已有密钥。
安装更新包并重启 API；之后配置变化会在请求时读取。网页自动进入文献库，无需输入密钥，
OpenAPI 也不再要求认证。所有能访问端口的客户端均可查询、管理导出及下载文件。
数据库凭据、超时、上传限制及已配置的资源控制保持生效。
改回 `"auth_required": true` 并刷新网页即可恢复密钥要求。

```bash
curl http://POD_IP:8088/status
```

## 接入已有部署

检出稳定版不会自动升级内部 `0.1.0` 部署。
输入快照、元数据代、已发布索引分别具有独立身份，应保留指针、清单、回执、引擎版本及父分段。
接入前阅读[发布与运维](RELEASE.zh-CN.md)。

`oa adopt --source-workspace /existing/workspace --generation GEN --build BUILD --port 8089`
将一个**新的** workspace 绑定到受支持、已冻结且已发布的 build，创建独立 API 凭据和指针，并链接源产物。
仅接受显式登记的语义等价投影。它不能管理共享数据库进程或构建新数据。
启动自己的 API 后，分别运行 `oa verify` 和 `oa restore`，再判断是否通过接入验收；绑定成功本身不是验收结果。
源元数据／检索后台及原路径必须仍可达，此操作不等于可迁移备份或数据库完整重部署。

作者／机构目录扩建、持久批量导出和导出 worker 见[检索与导出](SEARCH-EXPORT.zh-CN.md)。
