# 全索引文献版本匹配（v0.6 候选版）

[English](VERSION-MATCHING.md) · [版本详情](VERSIONS.zh-CN.md)

这项可选流程关联 OpenAlex 中彼此独立的文献记录，复用已发布检索索引，
并保留父版本的全部已确认关联，包括地学范围以外的 arXiv 关联。
原始文献 JSON、参考文献、OA、引用计数、检索分页和导出 ID 均保留原记录语义。
后台匹配可以与现有服务并行运行；软件升级与关联数据发布分别验收。

## 输入和处理步骤

前置条件是已验证的元数据代、已发布文本索引、已验证分类集合，以及已发布的
父关联版本。当前实现要求存在父版本，可先通过既有 arXiv 关联流程生成。

1. **特征目录：**顺序读取 Manticore 中的标题、完整作者身份、年份、类型和标识符，
   写入独立 ClickHouse 目录。不重建文本索引，不逐篇调用在线 API。
   每批写入回读验证后保存检查点和内容摘要，中断后可以幂等重放。
2. **试点：**仅对文献 ID 排序，查询限制为 8 GiB、两个线程；固定选中 ID 的回执后，
   分批读取特征并回读核验、保存检查点，避免全库完整内容参与排序导致内存超限。
   从整个已发布文献索引按确定性哈希均匀选取约百万篇，独立于旧版十万篇试点。
   后续批次排除已处理记录。
3. **候选生成：**使用规范化标题、作者与标题内容词组合，并为没有候选的预印本提供
   有界的原生标题词检索。组合采用最长的四个有效词近似稀有词，尚未实现全库词频筛选。
   DOI 候选可查询已导入的全量元数据，但共享 DOI 本身仍需审核。
   既有精确 arXiv 关联从父版本继承；标题检索仅覆盖已发布索引。
4. **校准审核：**按研究关联的连通分量划分，准备开发集 500 对、盲审新增自动关联
   600 对、困难负例 400 对。试点候选不足时，门槛保持未完成，不能视为通过。
5. **关联与验收：**在父版本完整快照上增补通过独立校准且直接验证的发表版关联。
   每个新成员必须直接匹配发表版锚点；竞争发表版和跨既有版本组连接保留待审。
   发布前验证覆盖数量和版本组成员的双向一致性。

特征、候选算法和确认规则分别记录身份。特征目录包含候选倒排投影，因此修改该投影
需要新目录；仅修改确认策略可复用兼容特征。只为拟确认关联和索引外 DOI 候选读取
完整原始记录，避免全量随机读取 JSON。引用中的特征库、检查点、父版本和索引依赖
都需要长期保留。

## 自动关联边界

候选年份窗口为 ±10 年；缺失年份保持待审。每篇最多保留 200 个候选，达到截断条件
标为 `overflow_review`，不能计为未匹配。作者姓名只作弱证据；要求共同 ORCID，或
至少两个稳定作者 ID；两边都为单作者且共享同一作者 ID 时例外。
自动关联仅允许预印本与发表版。更正、撤稿、数字或否定词冲突、DOI 歧义、重复发表和
扩展论文等仍需审核。

标题相似度是可解释的字符串分数，**不是正确概率**。当前提供
0.99／0.98／0.97／0.96 的诊断网格；首轮盲审将自动接受阈值固定为 **0.99**，
标题词重合率至少 **0.85**，候选分数间隔至少 **0.03**，竞争锚点按网格最宽松阈值检查。
降低阈值需要新规则和新的独立盲审，不能利用已经看过的盲审标签调参。

人工审核需标注 `same_study`、`different_study` 或 `uncertain`，填写审核人，
并声明 `independent_review: true`。不得生成标签冒充审核。
600 对盲审必须全部为可自动确认的真实关联，困难负例不能发生自动误合并。
600 对零错误对应单侧 95% 精度下界约 99.5%，不代表全库召回率。
只有至少获得 100 个盲审样例的分层可自动确认。
失败或已使用的盲审不能通过改标签重试；兼容后续批次可继承已通过校准及其来源证明。

## 运行方法

使用已安装的 `oa` 并指定正确工作目录。示例名称可自行替换；本地路径和实际数据规模
不写入社区 Git。

```bash
oa versions plan --profile full-index --release earth_match_pilot \
  --pilot-size 1000000 --workers 4
oa versions run --release earth_match_pilot --until review
oa versions status --release earth_match_pilot
oa versions stop --release earth_match_pilot
oa versions resume --release earth_match_pilot --until review
```

`var/versions/<release>/` 下生成 `review-development.json`、`review-blind.json`，
以及含固定分组和分数的内部 `review-package.json`。审核人使用前两个文件，
盲审人不应查看内部角色映射和分数。汇总标签文件包含 `identity`、
`independent_review: true` 和 `pairs`（每条含 `id`、`label`、`reviewer`）。

```bash
oa versions review-import --release earth_match_pilot --labels /local/reviews.json
oa versions run --release earth_match_pilot --until validate
oa versions publish --release earth_match_pilot
oa versions plan --profile full-index --release earth_match_core \
  --stratum 01_core_journals --batch-size 5000000
```

后续按核心期刊、作者支持、泛地学顺序分批执行，每批约五百万篇，分层名以固定索引为准。
每个新版本分别走完相同步骤。默认 `run` 停在审核阶段，没有自动绕过审核的发布。
未提供标签时仍可生成并发布“父版本确认关联＋新待审候选”的兼容快照，
但不会因此确认新增文献匹配。

## API、网页与运维

- `/v1/works/{id}/version-group`：确认组和原始命中记录；新格式额外区分已处理和未处理。
- `/v1/works/{id}/version-candidates`：候选证据、审核状态和处理状态；支持
  `relationship_release`、`per_page`，游标固定关联版本和查询文献。
- `/v1/version-groups/{id}`：当前关联版本中的原始成员元数据。
- `/v1/works/{id}/versions`：继续保留 arXiv 来源关联语义。

网页区分已确认成员和待审候选，支持查看证据，不跨版本转移参考文献。
特征覆盖、匹配覆盖、审核和发布是不同状态；扫描完成不代表每篇都有另一版本。

进度记录已提交数量、候选数、溢出数、处理字节、工作线程、速度和 ETA。
先保守启动，观察检索延迟后再增加并发。同 Pod 检查不能证明跨 Pod 可达性或高并发容量。
本流程无需重启元数据或检索引擎，更不能绕过校验和检查。升级时保留兼容的旧运行环境
及关联指针用于回滚。
