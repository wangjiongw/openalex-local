# 文档导航

[English](INDEX.md) · [项目总览](README.zh-CN.md)

OpenAlex Local 从本地快照建立元数据与引用服务，并允许按需要增加文本索引。
建议按下面的顺序了解和部署；可以先使用元数据服务，再决定文本检索范围。

## 按使用流程阅读

| 你想了解的问题 | 阅读入口 |
| --- | --- |
| 项目能做什么，哪些环节可选？ | [项目总览](README.zh-CN.md)、[功能清单](FEATURES.zh-CN.md) |
| snapshot、workspace、generation 和 build 分别是什么？ | [架构与核心概念](ARCHITECTURE.zh-CN.md) |
| 应下载什么文件、存放在哪里？ | [快照格式与获取](SNAPSHOT.zh-CN.md) |
| 如何启动本地服务？ | [Linux 原生部署](QUICKSTART.zh-CN.md) |
| 能检索哪些字段、查询哪些关系？ | [查询覆盖](API-COVERAGE.zh-CN.md)；运行服务的 `/docs` 和 `/capabilities` |
| 如何导出完整结果集或 DOI 清单？ | [检索与持久导出](SEARCH-EXPORT.zh-CN.md) |
| 如何保留数据、重启、迁移或回滚？ | [发布与运维](RELEASE.zh-CN.md) |
| 哪些能力已经经过实际验证？ | [验证证据与边界](VALIDATION.zh-CN.md) |
| 后续计划是什么，官方项目有哪些参考价值？ | [路线图](ROADMAP.zh-CN.md)、[官方仓库参考](FEATURES.zh-CN.md#3-官方仓库可参考的扩展) |
| 如何参与开发？ | [贡献指南](../CONTRIBUTING.zh-CN.md)、[变更记录](../CHANGELOG.zh-CN.md) |

## 版本与开发工具

- 当前稳定版：[v0.3.2](releases/v0.3.2.zh-CN.md)。上一稳定版：[v0.3.1](releases/v0.3.1.zh-CN.md)。
- [项目当前状态](STATUS.zh-CN.md)：已交付流程、已知限制与维护范围。
- 发布记录：[v0.3.0-rc.1](releases/v0.3.0-rc.1.zh-CN.md)、[v0.2.0-rc.1](releases/v0.2.0-rc.1.zh-CN.md)。
- 面向读者的文档提供中英文配对版本，通过语言链接切换。代码、命令、API 字段和标识符在两版中保持相同写法。
- [AGENTS.md](../AGENTS.md) 和 [Agent 实现资料](agents/CODE-MAP.md) 是英文单份的开发工具指令，提供代码导航和执行约束；项目设计与使用流程以本导航中的说明为入口。
- 源码公开；快照、凭据、索引、引擎、导出与部署报告留在本地 workspace。源码发布包不等于数据库备份。
