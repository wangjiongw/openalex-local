# OpenAlex Local

[English](../README.md) · [文档导航](INDEX.zh-CN.md)

使用已下载的 OpenAlex 快照搭建本地文献元数据与引用服务。
工具链读取本地文件、保留完整记录，并提供网页和 HTTP API；按需要增加索引，实现高效文本检索。

**稳定版：v0.3.2；包／API：`0.3.2`。**
从[原生部署](QUICKSTART.zh-CN.md)开始；实际测试范围见[验证说明](VALIDATION.zh-CN.md)，不能默认完全对齐实时官方 API。

当前能力、交付边界和已知限制汇总见[项目状态](STATUS.zh-CN.md)。

## 工作流程

1. 按[格式与获取指引](SNAPSHOT.zh-CN.md)自行下载一致的快照。
2. 指定本地数据路径，校验、导入并审计记录、DOI／ID 映射和 ClickHouse 中的正反向引用关系。
3. 发布元数据服务；DOI／ID、完整记录和引用浏览不需要文本索引。
4. 可选运行地学示例，构建、验收 Manticore 索引，再发布标题、摘要、关键词和作者姓名搜索。
5. 按需要扩建实体目录，并经 API、CLI 或网页导出查询结果或 DOI／ID 清单。

默认 `metadata` profile 与学科无关。`earth_sciences` 是当前唯一内置的文本 profile，作为可选示例提供。
论文即使不在已发布文本索引中，仍可通过已导入元数据解析。
相关概念见[架构与术语](ARCHITECTURE.zh-CN.md)。

## 已提供的能力

- 官方风格路由与本地 `/v1` 扩展、已支持的组合过滤、引用展开、实体目录和浏览器界面。
- 可取消、恢复并下载的持久 JSONL.gz／CSV／DOI／ID 导出。
- 可恢复的独立模块与 pipeline、固定原生引擎、显式验收与发布，以及搜索恢复。
  支持 Linux x86_64／ARM64，旧 ARM CPU 需使用注明的兼容 profile。
- 内部部署与社区用户共用一套维护包；本地数据、密钥、资源和生效指针由 workspace 管理。

快照可放在 workspace 外。数据、索引、凭据、引擎二进制、环境和报告不加入 Git。
PDF／TEI 下载、自动版本归组、语义检索和连续快照更新仍待开发。
完整导入元数据不代表所有字段都有检索索引。

## 开发与进一步阅读

```bash
python3 -m venv .venv
.venv/bin/pip install -e . pytest
export OPENALEX_HOME="$(mktemp -d /tmp/openalex-dev.XXXXXX)"
.venv/bin/python -m openalex_api.cli init
.venv/bin/python -m pytest -q
```

包名为 `openalex-local-api`，Python 导入名为 `openalex_api`，CLI 为 `oa`。
源码与运行 workspace 分开管理。

阅读[文档导航](INDEX.zh-CN.md)、[功能清单与官方参考](FEATURES.zh-CN.md)、
[API 覆盖](API-COVERAGE.zh-CN.md)、[检索与导出](SEARCH-EXPORT.zh-CN.md)、
[运维](RELEASE.zh-CN.md)、[路线图](ROADMAP.zh-CN.md)和[贡献指南](../CONTRIBUTING.zh-CN.md)。
Agent 开发工具从 [AGENTS.md](../AGENTS.md) 进入，再按任务读取实现资料。

## 许可与项目关系

源码采用 [MIT](../LICENSE)。OpenAlex 数据采用独立的 CC0 许可；应从官方获取并保留许可与发布说明。
ClickHouse、Manticore、AWS CLI 及其依赖遵守各自许可。
本仓库不重新分发引擎二进制或生产数据。这是独立社区项目，不是 OpenAlex 官方服务。
