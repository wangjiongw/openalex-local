# OpenAlex 文献版本关联

[English](VERSIONS.md) · [API 覆盖](API-COVERAGE.zh-CN.md) · [开发设计](VERSION-GROUPS-PLAN.zh-CN.md)

可选版本流水线复用已发布 arXiv 来源中验明的精确 DOI 证据，关联彼此独立的
OpenAlex 记录。所有原始 W ID、字段和参考文献列表均保留。检索数量与分页不变。
安装软件、发布关系数据和升级服务是三个独立步骤。

## 输入与可复用阶段

前提是已验收的元数据 generation、已发布的 [arXiv 来源](ARXIV.zh-CN.md)，以及
完整验收的地学集合与已发布 Manticore 成员回执。首批新证据处理有意限定为地学试跑，
无需重建文献文本索引，日常查询也不访问外部数据源。

```bash
oa --workspace /path/to/workspace versions plan --release earth_versions_v1
oa --workspace /path/to/workspace versions run --release earth_versions_v1
oa --workspace /path/to/workspace versions status --release earth_versions_v1
oa --workspace /path/to/workspace versions validate --release earth_versions_v1
oa --workspace /path/to/workspace versions publish --release earth_versions_v1
```

计划通过 `--generation`、`--arxiv-release`、`--collection`、`--seed` 选择并冻结输入；
默认随机种子为 `20261113`。输入或处理签名变化须另建 release。
`run --until <stage>` 可分阶段执行，各阶段都有持久检查点：

| 阶段 | 处理及持久结果 |
| --- | --- |
| `seeds` | 对全部已选 arXiv 记录重放精确关联，对照冻结的标识符与目标证据，保留多成员和歧义决策 |
| `sample` | 从已验证回执确定性抽样：核心期刊 3 万、泛地学／交叉学科 3.5 万、作者扩展 3.5 万；保存 ID、分层、源散列及样本散列 |
| `targets` | 核对所有种子目标和试跑文献确实存在于固定元数据中，比对源标识字段及试跑原始记录散列 |
| `pilot` | 提取重复 DOI 与显式 arXiv 链接候选；对没有重复 DOI 候选的抽样预印本复用标题、作者、年份候选检索 |
| `groups` | 只确认既有精确证据支持、发表版无冲突的版本组；保留待审与冲突关系 |
| `store` | 写入独立 ClickHouse 版本组、成员及待审表，核对写入并支持安全重放 |
| `validate` | 再核验冻结输入、全部种子及目标处理完整性、样本身份、全部关系行和成员映射，之后才允许发布 |

`stop` 在下一安全批次边界暂停；`resume` 清除暂停请求并复用已提交批次。
终止错误会保留在 `status` 中。一个 workspace 只允许一个任务写入者。
试跑在每个冻结层内均匀抽样，不代表全 OpenAlex，也不是经过人工标注的匹配评测集。
新试跑证据只处理选中的 10 万条；既有 arXiv 种子覆盖该来源的全部学科。

## 保守确认规则

来源至少须对应两个不同 W ID，且只有一个无歧义发表版，才形成确认组。
只有预印本时不虚构发表版。相同发表版的关联可汇合；若成员存在互相竞争的发表版，
相关组进入待审，不沿链条自动合并。撤稿及更正、勘误通知作为待审关系保留，不自动首选。

重复 DOI、显式链接、标题及作者相似均为候选，除非既有精确证据独立支持该关联。
不同标题可能是同一研究的版本，相同标题也可能是不同论文。
本次不新增模糊自动确认阈值，也不宣称完成全部地学文献的版本匹配。

## API 与网页

- `GET /v1/works/W123/version-group`：返回请求原始记录、关系 release、成员及每个成员的
  完整记录、证据和 `preferred_openalex_id`。有效但未成组的记录返回 `state: unmatched`。
- `GET /v1/version-groups/vg_<hash>`：返回当前已发布关系 release 的版本组 JSON。
  本地组 ID 与 release 绑定。
- 既有 `GET /v1/works/W123/versions` 仍返回 arXiv 来源记录，语义不变。
- `/status.version_groups`、`/capabilities.version_groups` 报告可选模块就绪状态、
  种子处理、试跑范围和统计。模块未发布或损坏不会阻断普通 OpenAlex 查询。
  关系模块不可用时新接口返回 503；记录或组不存在时返回 404。

网页默认展示唯一确认发表版，并标明命中与当前展示记录。手动选择版本后不会自动跳回。
可以检查证据并下载版本组 JSON。参考文献、作者、摘要、OA、DOI、许可及日期始终属于
当前展示版本；不叠加不同版本的被引数。已有勾选导出和查询导出仍按原始记录工作；
后台分组导出和分组搜索留待后续。

## 存储与回滚

状态位于 workspace 的 `var/versions/<release>/`：冻结计划、持久 `staging.sqlite`、
`sample.json`、验收及 `lineage.json`。ClickHouse 使用独立的
`oa_versions_<release>.groups`、`.members`、`.reviews`，不会改写原始元数据与引用边。
这些文件、表和固定的 arXiv／元数据／索引依赖均须保留，且不加入 Git。

`var/versions-active.json` 保留前一指针。需要回退时，用兼容软件重新验收并发布保留的
旧 release，核对关系后再切换指针。沿革文件记录旧组与按共同成员找到的新组，包括退出
和拆分；它不是 OpenAlex 官方合并映射。旧软件环境也要保留；软件与关系数据分别回滚。

只有一条 OpenAlex 目标的来源关联同样遵守 `automatic_display_allowed`：更正、撤稿等记录不自动跳转展示。

[全索引匹配候选流程](VERSION-MATCHING.zh-CN.md)
