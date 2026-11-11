# v0.3.0-rc.1 范围与运维

[English](RELEASE.md) · [文档导航](INDEX.zh-CN.md)

Git 标签为 `v0.3.0-rc.1`，包／API 版本为 `0.3.0rc1`（PEP 440）。
这是既有工具链的 GitHub 预发布版本，不是一份新的生产数据集。
已有数据需单独检查兼容性并受控发布，发布源码不会自动升级其他部署。

## 当前公开流程

- 使用者按照[格式指引](SNAPSHOT.zh-CN.md)自行下载一致的快照。
- 读取／校验本地新旧 JSONL，将完整记录、标识符和正反向引用关系导入 ClickHouse。
- 默认发布不依赖 Manticore 的元数据 API／网页服务。
- 可选地学分类／导出、Manticore 文本索引构建、验收、恢复及显式发布；复用已有元数据。
- 查询 DOI／ID、完整源记录、参考／被引／相关文献及已支持的索引文本和过滤条件；通过 capabilities 确认能力，不假设完全兼容官方。

本候选版在保留元数据和论文索引投影的基础上，增加实体目录、查询控件与持久导出。
完整查询边界见[API 覆盖](API-COVERAGE.zh-CN.md)。

## 受支持命令入口

| 用途 | 命令 |
| --- | --- |
| Workspace 与输入 | `init --snapshot`、`inventory`、`snapshot verify [--deep]` |
| 原生引擎 | `engines ensure`；可选 `--search` 安装 Manticore；支持离线缓存和明确的平台 profile |
| 生命周期链路 | `pipeline plan/run/resume/status --generation ...`；默认 `metadata`，可选 `--profile earth_sciences` |
| 复用导入元数据 | `search-catalog`、`earth-export`、`earth-export-status`、`earth-export-stop`、`earth-export-verify` |
| 索引构建与发布 | `search-build`、`search-validate`、`search-publish`、`search-status` |
| 目录扩建 | `catalogs build/status/stop/validate/publish` |
| 结果导出 | `export create/list/status/cancel/resume`；`start export-worker` |
| 已有数据检查 | `adopt`、`verify`、`restore`（搜索恢复只读使用已有元数据服务） |
| 服务运维 | `start/stop/restart clickhouse`、`manticore`、`api` 或 `export-worker`；`status`、`logs`、`doctor` |

在命令前加 `oa --workspace /absolute/workspace`。示例与必需参数见[部署指南](QUICKSTART.zh-CN.md)，
具体语法由 `oa COMMAND --help` 展示。
原始 `oa manticore` 和历史故障处理命令属于高级／兼容入口，不是另一套受支持部署流程。
旧 OpenSearch／Lucene 迁移、canary、诊断和 `baseline/` 代码因仍有调用方与测试而保留，默认服务不使用它们。
不要对已接入或生产数据执行历史修复命令。

## 安装与持久状态

在虚拟环境中安装带标签源码或发布的 wheel，使用 `SHA256SUMS.txt` 校验附件。
当前通过 GitHub 分发，不要求 PyPI 发布，也不捆绑引擎／数据压缩包。

以下内容与 Git 分开保留：

- 原始快照、清单、许可与发布说明。
- 私有 workspace 配置、密钥与引擎安装身份。
- ClickHouse 持久数据及元数据 generation／检查点／审计回执。
- 集合导出、所有不可变索引快照、清单、回执和被引用父分段，包括新 build 仍依赖的旧分段。
- 目录身份到论文的派生 ClickHouse 表、身份检查点，以及固定数据／索引版本的导出任务和文件。
- 本地验收及运维报告。

单独的 wheel 或源码不能恢复文献数据库，Git 也不是数据备份。
整理源码时不能删除已发布版本的依赖。

## 重启、复用与回滚

迁移 workspace 前先停止旧 owner。包括数据库和导出 worker 在内的 writer 只能由一个主机持有，
不能让两个进程／Pod 打开同一 ClickHouse 数据目录。
只有确认旧服务停止后才能手动释放所有权标记。隔离 workspace 启动前应配置不冲突端口。

先启动 ClickHouse；有已发布文本索引时再启动 Manticore；然后启动 API 和导出 worker。
检查 `/healthz`、`/readyz`、带认证的 `/status` 与 `/capabilities`。
纯元数据部署可以在 `search_ready: false` 时就绪。API 运行不代表所有文本范围都有索引。

兼容代码／包安装不要求重建既有不可变索引。
`adopt` 将隔离 API 绑定到明确受支持的冻结产物，不复制数据库，也不验收一个新的可写部署。
对接入 API 做功能检查，并保持源部署可用。

评估候选版时保留原 API；放弃评估只停止候选 API。
升级独立部署前，保留旧软件、配置和指针。
重新选择旧数据／索引版本前，需要验证目标与所选代码兼容；仅切换 Git 标签不是合格回滚。
不要手工修改已发布回执或绕过签名校验，让旧索引看似兼容。

## 验证与已知限制

[验证记录](VALIDATION.zh-CN.md)区分内部全量证据、社区合成原生样例及候选版兼容测试。
复用既有规模和搜索恢复证据，不为候选版重复构建或重索引生产语料。
最终查询／负载报告应绑定未变化的候选代码及相应生产指针／清单。

- 文本搜索只覆盖已发布索引，地学候选不等于全部学科。
- 保存原始 JSON 不代表每个字段都有过滤索引。已有部分元数据统计／抽样，不支持任意文本＋聚合组合。
- 书目文本匹配返回未校准候选；尚无版本归组、发表版优先或自动去重合并。
- 源摘要、参考文献或内容可能缺失；重建索引不能恢复源快照中不存在的信息。
- PDF／TEI 下载、Parquet 导入、连续更新、删除／合并 ID、语义检索和 SDK／MCP 是后续可选工作。
- 现有搜索快照已通过全量恢复检查；没有认证独立 ClickHouse 灾备或真实 Pod 丢失恢复。
- 内部历史 ClickHouse checksum 故障的根因尚未关闭；源码发布不声称已修复根因，当前若复现存储错误仍须报告。

没有强制 24 小时观察要求。候选版反馈、修复和验证完成后可另行考虑稳定版。
发布预发布版本不会自动切换内部服务。

## 检索与导出候选版

新增功能、命令、worker 生命周期和覆盖边界见[检索与导出](SEARCH-EXPORT.zh-CN.md)。
新增目录需要独立验收后发布，复用原论文分段和元数据 generation。
