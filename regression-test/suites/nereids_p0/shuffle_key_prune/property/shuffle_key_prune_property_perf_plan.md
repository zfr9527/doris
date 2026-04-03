# Shuffle Key Prune 性能专项测试方案

## 1. 文档目标

这份文档只讨论一件事：

- `shuffle key prune` 在已经满足裁剪条件时，是否带来性能优化
- 或者在某些边界输入下，是否会出现性能劣化

它不是功能覆盖文档，也不追求把所有规则路径穷举出来。

这份文档关注的核心问题是：

- 在同一类 SQL 形态下，`enable_agg_shuffle_key_prune=false/true` 的性能差异有多大
- 哪些变量会放大收益
- 哪些变量会导致收益变弱，甚至出现劣化
- 不同数据类型被选成最终 shuffle key 时，性能差异如何
- parent request / join parent subset 下推后，是优化更多，还是因为 key 过窄产生退化

## 2. 设计原则

### 2.1 以性能变量实验为中心

这批测试不按“功能 case 名称”来组织，而按“性能变量”来组织。

每个性能 case 都遵循下面的原则：

1. 先确保 `prune` 可以发生
2. 固定 SQL 形态和大部分统计语义
3. 只改变一个关键变量
4. 比较 `prune off` 与 `prune on` 的性能
5. 或者在 `prune on` 条件下，比较不同数据类型的绝对性能

### 2.2 功能正确性只保留最小护栏

这批测试不是功能专项，所以功能断言只保留最小闭环：

- `enable_agg_shuffle_key_prune=false/true` 的查询结果一致
- explain 中 `orderedShuffledColumns` 的变化符合预期
- 目标 case 确实发生了：
  - `single-key prune`
  - `reduced multi-key`
  - `plan unchanged`
  - `parent subset request`

除此之外，不再把大量功能语义断言塞进性能文件。

### 2.3 默认就是 heavy

这批性能专项不再提供轻量默认值。

默认物理数据量统一按 heavy 模式建模：

- 单表默认 `100,000,000` 行
- join/parent 场景的左右表默认都按 `100,000,000` 行设计
- `statsRowCount` 和 `physicalRowCount` 对齐

### 2.4 避免重复造多张 1 亿行大表

性能专项应该测执行差异，而不是测重复造数成本。

因此每个文件只允许：

- 造 `1` 张或 `2` 张大表
- 多个变量实验复用同一张大表
- 用不同 SQL 和不同 set stats 驱动不同性能场景

不建议把每个 case 都做成一张独立的 `1e8` 行表。

### 2.5 只放到 property 目录，和功能 suite 隔离

后续所有性能专项文件都单独放到：

- `regression-test/suites/nereids_p0/shuffle_key_prune/property`

不和现有：

- `shuffle_key_prune_hotvalue_*`
- `shuffle_key_prune_prod_attention_*`
- `shuffle_key_prune_join_*`

这些功能文件混在一起。

## 3. 相关源码语义

这批性能实验要对齐的不是“case 名”，而是源码里的性能相关决策点。

### 3.1 低 NDV 风险

低 NDV 门槛来自：

- [AggregateUtils.java](/Users/zhangfurong/Documents/work/my_repo/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/util/AggregateUtils.java#L51)

当前固定值是：

- `LOW_NDV_THRESHOLD = 1024`

### 3.2 skew 风险

skew 检查逻辑可以参考：

- [ChildrenPropertiesRegulator.java](/Users/zhangfurong/Documents/work/my_repo/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/properties/ChildrenPropertiesRegulator.java#L161)

这里的重点不是功能语义本身，而是：

- 某个候选 key 虽然“合法”
- 但如果它处在边界附近，可能仍然带来实际执行长尾

这正是性能专项里“劣化场景”的来源之一。

### 3.3 parent subset 下推门槛

parent request 继续下推的门槛可以参考：

- [RequestPropertyDeriver.java](/Users/zhangfurong/Documents/work/my_repo/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/properties/RequestPropertyDeriver.java#L485)

这里的关键点是：

- parent key 是 child group-by key 的子集时，可能继续把更窄的 request 下推
- 但是否值得下推，要看 `combinedNdv`
- 当前门槛仍然是 `combinedNdv > 1024`

这意味着：

- parent key 缩窄以后可能继续优化
- 也可能因为 key 过窄而在真实数据上产生性能劣化

## 4. 性能测试真正关注的变量

本专项把性能变量分成四大类。

### 4.1 变量 A：被裁掉的列数

核心问题：

- 从 `4 列 key` 缩到 `1 列 key`，收益是否明显大于从 `2 列 key` 缩到 `1 列 key`

本质上，这个变量对应：

- shuffle key 序列化成本
- hash key 比较成本
- exchange 数据宽度
- agg hash table key 宽度

### 4.2 变量 B：最终选中的 key 有多“安全”

核心问题：

- 同样都满足 prune 条件，边界内但“勉强安全”的 key，会不会比“明显安全”的 key 更容易产生长尾

这个变量的设计重点不是验证功能对错，而是验证：

- 合法 prune 不一定总是赚
- 某些边界内 key 可能收益很小
- 极端情况下甚至可能劣化

### 4.3 变量 C：最终选中的 key 的数据类型

核心问题：

- 最终选中 `bigint`、`datev2`、`char(8)`、`varchar(16)`、`varchar(128)`、`string` 时，性能差距有多大

这组实验强调的是：

- 同样发生 prune
- 不同数据类型的 hash / compare / serialize 成本不同
- 最终收益会显著不同

### 4.4 变量 D：parent subset 收缩宽度

核心问题：

- parent key 下推后，child 侧 request 更窄了
- 这是继续优化，还是因为 key 过窄导致分布变差

这组实验主要针对：

- parent agg
- join parent request
- mixed join / parent subset

## 5. 文件拆分方案

建议后续在 `property` 目录中创建以下文件：

- `shuffle_key_prune_property_perf_plan.md`
- `agg_shuffle_key_prune_perf_key_width_heavy_manual.groovy`
- `agg_shuffle_key_prune_perf_valid_margin_heavy_manual.groovy`
- `agg_shuffle_key_prune_perf_datatype_heavy_manual.groovy`
- `agg_shuffle_key_prune_perf_parent_subset_heavy_manual.groovy`

四个 groovy 文件分别对应四类性能变量。

## 6. 统一执行基线

所有 heavy manual 性能 suite 建议统一使用以下默认配置：

- `row_count = 100000000`
- `repeat_times = 5`
- `warmup_times = 1`
- `parallel_pipeline_task_num = 8`
- `enable_nereids_planner = true`
- `enable_fallback_to_original_planner = false`
- `enable_sql_cache = false`
- `enable_query_cache = false`
- `agg_phase = 0`
- `enable_agg_shuffle_key_prune = false/true` 交替比较

建议执行顺序：

1. warm up `prune off`
2. warm up `prune on`
3. 正式轮次中交替执行 `off/on`
4. 每个 case 取 `median` 作为主比较值
5. 同时记录 `min` 作为补充信息

## 7. 统一观测指标

### 7.1 主指标

所有 case 都统一记录：

- `offMedianMs`
- `onMedianMs`
- `offMinMs`
- `onMinMs`
- `speedupRatio = offMedianMs / onMedianMs`

### 7.2 次指标

如果 profile 指标能稳定抓取，则额外记录：

- exchange bytes
- fragment/agg 节点 peak memory
- 长尾 instance 最大耗时

### 7.3 结果判读原则

第一版不写死“必须优化多少”的强阈值。

主要输出结论应是：

- 明显优化
- 轻微优化
- 基本持平
- 轻微劣化
- 明显劣化

这样更符合 heavy 环境下的真实波动情况。

## 8. 文件一：Key Width 性能实验

文件名建议：

- `agg_shuffle_key_prune_perf_key_width_heavy_manual.groovy`

### 8.1 测试目标

验证：

- prune 前后减少的 key 列数越多，收益是否越明显

### 8.2 核心变量

只改变：

- 原始 group-by key 宽度

固定不变：

- 最终保留下来的安全 key
- 数据类型
- hot/null/ndv 语义

### 8.3 建议表结构

建议只造一张 `100,000,000` 行窄表，包含：

- `x`
- `hot_key`
- `good_key`
- `k3`
- `k4`
- `k5`
- `v`

### 8.4 建议 case

- `KW01`: 原始 `4 列`，最终裁到 `1 列`
- `KW02`: 原始 `3 列`，最终裁到 `1 列`
- `KW03`: 原始 `2 列`，最终裁到 `1 列`

### 8.5 预期

- `KW01` 收益应最大
- `KW02` 次之
- `KW03` 收益最弱

### 8.6 可能的劣化场景

如果原始 `2 列 key` 本身都是窄数值列，且第二列并不造成明显负担，那么：

- `KW03` 很可能只带来极弱收益
- 在某些环境下甚至可能接近平手

这类 case 不属于异常，而是性能特征。

## 9. 文件二：Valid Margin 性能实验

文件名建议：

- `agg_shuffle_key_prune_perf_valid_margin_heavy_manual.groovy`

### 9.1 测试目标

验证：

- 同样都满足 prune 条件
- “明显安全”的 key 和“边界内勉强安全”的 key，性能收益差异有多大

### 9.2 核心变量

只改变：

- 最终被选中 key 的安全裕量

固定不变：

- SQL 结构
- 原始 group-by 列数
- 最终目标仍然都是 prune 到单列 key

### 9.3 建议分组

- `VM01`: 高裕量
  - `hot = 0`
  - `ndv` 远高于门槛
- `VM02`: 中裕量
  - `hot` 很低
  - `ndv` 明显高于门槛
- `VM03`: 低裕量
  - `hot ≈ 4.9%`
  - `ndv` 略高于门槛

### 9.4 预期

- `VM01` 往往有最稳定的收益
- `VM02` 收益仍应明显，但弱于 `VM01`
- `VM03` 可能出现：
  - 收益明显变小
  - 持平
  - 轻微劣化

### 9.5 这组实验的价值

这组实验是整套性能专项里最重要的一组“劣化设计”。

它回答的问题是：

- 不是所有合法 prune 都一定赚钱
- 边界内的 key 也可能在真实执行上不够好

## 10. 文件三：Datatype 性能实验

文件名建议：

- `agg_shuffle_key_prune_perf_datatype_heavy_manual.groovy`

### 10.1 测试目标

验证：

- 最终被选中的 shuffle key 数据类型不同，性能差异如何

### 10.2 核心变量

只改变：

- 最终被选中的 key 类型

固定不变：

- SQL 结构
- 裁剪前的 group-by 宽度
- hot/null/ndv 语义

### 10.3 建议表结构

建议只造一张 `100,000,000` 行宽表，包含：

- `li_key`
- `d_key`
- `dt_key`
- `ch8_key`
- `vc16_key`
- `vc128_key`
- `s_key`
- `hot_guard_key`
- `v`

### 10.4 建议 case

- `DT01`: 最终选中 `bigint`
- `DT02`: 最终选中 `datev2`
- `DT03`: 最终选中 `char(8)`
- `DT04`: 最终选中 `varchar(16)`
- `DT05`: 最终选中 `varchar(128)`
- `DT06`: 最终选中 `string`

### 10.5 预期

- `bigint/datev2` 这类固定宽度类型通常收益最好
- `char(8)` 和 `varchar(16)` 次之
- `varchar(128)` 和 `string` 即使发生 prune，收益也可能弱很多

### 10.6 劣化场景

如果最终选中的是：

- `varchar(128)`
- `string`

则可能出现：

- prune 虽然发生
- 但 hash key 仍然很重
- 最终收益明显不如数值型
- 某些情况下甚至可能比 full key 更差

这组实验能解释“为什么不同类型的 prune 收益不一样”。

## 11. 文件四：Parent Subset 性能实验

文件名建议：

- `agg_shuffle_key_prune_perf_parent_subset_heavy_manual.groovy`

### 11.1 测试目标

验证：

- parent request / join parent subset 下推之后，是继续优化，还是因为 key 过窄而退化

### 11.2 核心变量

只改变：

- parent 下推后的 subset 宽度和 subset NDV

固定不变：

- child 侧完整 group-by key
- join / parent SQL 形态

### 11.3 建议表结构

建议使用两张 `100,000,000` 行表：

- `fact_left_100m`
- `probe_right_100m`

### 11.4 建议 case

- `PS01`: parent subset 为 `2 列`，且 NDV 高
- `PS02`: parent subset 为 `1 列`，且 NDV 高
- `PS03`: parent subset 为 `1 列`，且 NDV 仅略高于门槛

### 11.5 预期

- `PS01` 往往最稳
- `PS02` 可能继续优化
- `PS03` 是重点劣化观察点

### 11.6 重点解释

这组实验不是测“有没有交集”，而是测：

- 交集有了以后，缩窄到什么程度还划算

因此它和现有 `join_parent` 功能 suite 的目标完全不同。

## 12. 查询模板建议

为了避免结果集传输干扰性能结论，建议统一采用“两层结构”：

```sql
select count(k1), count(k2), count(sum_v)
from (
    select k1, k2, sum(v) as sum_v
    from t
    group by k1, k2
) t
```

或者：

```sql
select count(*), max(sum_v)
from (
    select k1, k2, k3, sum(v) as sum_v
    from t
    group by k1, k2, k3
) t
```

这样做的目的是：

- 让 shuffle / agg 足够重
- 避免把结果集回传成本混进主结论

## 13. 断言策略

### 13.1 强断言

性能文件中建议保留的强断言只有：

- `off/on` 结果一致
- explain 中 `orderedShuffledColumns` 符合预期
- 目标路径确实发生

### 13.2 弱断言

第一版不建议写死严格性能阈值。

如果后续确实需要做方向性保护线，可以考虑：

- 明显优化场景：
  - `onMedianMs` 不应比 `offMedianMs` 慢超过 `10%`
- 劣化观察场景：
  - 不做优化承诺
  - 只要求日志中明确输出性能结论

## 14. 与现有功能文件的关系

现有这些文件仍然是功能/语义参考：

- `shuffle_key_prune_hotvalue_threshold_boundary_case.groovy`
- `shuffle_key_prune_prod_attention_join_parent_case.groovy`
- `shuffle_key_prune_prod_attention_degrade_stats_case.groovy`
- `shuffle_key_prune_distinct_partition_case.groovy`
- `shuffle_key_prune_join_key_opt_case.groovy`
- `shuffle_key_prune_mixed_join_case.groovy`

但后续性能专项不会再按这些文件的 case 分类来设计。

性能文件只借它们的：

- SQL 形态
- 统计语义
- explain 解析方式

不会继承它们的功能目标。

## 15. 第一版实施顺序

建议按下面顺序落地：

1. `agg_shuffle_key_prune_perf_key_width_heavy_manual.groovy`
2. `agg_shuffle_key_prune_perf_valid_margin_heavy_manual.groovy`
3. `agg_shuffle_key_prune_perf_datatype_heavy_manual.groovy`
4. `agg_shuffle_key_prune_perf_parent_subset_heavy_manual.groovy`

顺序理由：

- `key_width` 最直观，最容易先得到稳定优化结论
- `valid_margin` 最能体现“合法 prune 不一定赚钱”
- `datatype` 最能解释不同类型的收益差异
- `parent_subset` 最复杂，适合最后落地

## 16. 一句话总结

这套性能专项的核心不是：

- “会不会 prune”

而是：

- “在 prune 已经发生时，哪个变量让它更赚，哪个变量让它不赚甚至变亏”

这也是后续所有 heavy 性能 case 的统一设计准则。
