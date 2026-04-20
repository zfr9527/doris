# multi distinct strategy change 正反例测试方案

## 1. 核心原则

这次测试方案按“同一个判断点同时设计正例和反例”的方式组织。

每个测试点都回答两个问题：

```text
同一个判断点
├── 正例：满足条件时，必须触发新逻辑
└── 反例：只差一个关键条件时，不能误触发新逻辑
```

本次改动的核心逻辑是：

```text
if distinct key satisfies hash distribution:
    use split strategy
else:
    use old strategy
```

所以主测试树只围绕两件事展开：

```text
主测试目标
├── 命中判断必须准：该满足分布时一定走 split
├── 反向约束必须准：不满足分布时一定回到旧逻辑
└── 聚合函数覆盖必须全：不能只验证 count(distinct ...)
```

统计信息和 NDV 不是新逻辑的条件，只是用来证明：

```text
统计信息 / NDV 的角色
├── 满足分布时：不能影响 split 选择
└── 不满足分布时：继续影响旧策略选择
```

聚合函数的角色：

```text
聚合函数维度
├── 必测主函数
│   ├── count(distinct ...)
│   ├── sum(distinct ...)
│   ├── sum0(distinct ...)
│   └── group_concat(distinct ...)
│
└── 测试要求
    ├── 同一个判断点至少要有 count 作为主证明点
    ├── 对支持 multi_distinct 改写的其他 distinct 聚合函数，要补函数级正反例
    └── 不能把函数覆盖全部降级到兼容性里
```

## 2. 测试数据树

```text
测试数据
├── t_md_hash_id
│   ├── 分布：distributed by hash(id)
│   ├── id：分布列，高 NDV
│   ├── g_low：低 NDV group by 列
│   ├── g_high：高 NDV group by 列
│   ├── dst_low：非分布列，低 NDV
│   ├── dst_high：非分布列，高 NDV
│   └── payload：普通聚合列
│
├── t_md_hash_ab
│   ├── 分布：distributed by hash(a, b)
│   ├── a：分布列之一
│   ├── b：分布列之一
│   ├── g：group by 列
│   └── v：普通聚合列
│
└── t_md_lineage
    ├── 分布：distributed by hash(k1)
    ├── k1：分布列
    ├── k2：Join / Filter 辅助列
    ├── g：group by 列
    └── v：普通聚合列
```

建表示例：

```sql
create table t_md_hash_id (
    id int,
    g_low int,
    g_high int,
    dst_low int,
    dst_high int,
    payload int
)
duplicate key(id)
distributed by hash(id)
properties("replication_num" = "1");

create table t_md_hash_ab (
    a int,
    b int,
    g int,
    v int
)
duplicate key(a, b)
distributed by hash(a, b)
properties("replication_num" = "1");

create table t_md_lineage (
    k1 int,
    k2 int,
    g int,
    v int
)
duplicate key(k1)
distributed by hash(k1)
properties("replication_num" = "1");
```

## 3. 正反例测试树

### T1. 判断对象必须是 distinct key，不是 group by key

```text
T1. 判断对象
├── 正例：distinct key 是分布列
│   ├── SQL：count(distinct id) group by g_low
│   ├── 条件：id 是 distributed by hash(id) 的分布列
│   ├── 预期：触发新逻辑，走 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：group by key 是分布列，但 distinct key 不是分布列
    ├── SQL：count(distinct dst_high) group by id
    ├── 条件：id 是分布列，但 dst_high 不是分布列
    ├── 预期：不能触发新逻辑，回到旧策略
    └── 断言：不能因为 group by 命中分布就强制 split
```

SQL 示例：

```sql
explain shape plan
select g_low, count(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select id, count(distinct dst_high)
from t_md_hash_id
group by id;
```

### T2. 满足分布时，统计信息不能改变 split 结论

```text
T2. 统计信息优先级
├── 正例：满足分布 + 无统计信息
│   ├── SQL：count(distinct id) group by g_low
│   ├── 条件：drop stats，id 是分布列
│   ├── 预期：仍然触发新逻辑，走 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：不满足分布 + 无统计信息
    ├── SQL：count(distinct dst_high) group by g_low
    ├── 条件：drop stats，dst_high 不是分布列
    ├── 预期：不能触发新逻辑，进入原 unknown stats 分支
    └── 断言：策略选择与旧逻辑一致
```

SQL 示例：

```sql
drop stats t_md_hash_id;

explain shape plan
select g_low, count(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, count(distinct dst_high)
from t_md_hash_id
group by g_low;
```

### T3. 满足分布时，NDV 组合不能改变 split 结论

```text
T3. NDV 覆盖
├── 正例：满足分布 + 低 group by NDV + 高 distinct NDV
│   ├── SQL：count(distinct id) group by g_low
│   ├── 条件：analyze 后，g_low 低 NDV，id 高 NDV 且是分布列
│   ├── 旧逻辑可能倾向：multi_distinct
│   ├── 预期：新逻辑优先，仍然 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：不满足分布 + 低 group by NDV + 高 distinct NDV
    ├── SQL：count(distinct dst_high) group by g_low
    ├── 条件：dst_high 高 NDV 但不是分布列
    ├── 预期：不能触发新逻辑，按旧逻辑选择
    └── 断言：如果旧逻辑应选 multi_distinct，则出现 multi_distinct_count
```

SQL 示例：

```sql
analyze table t_md_hash_id with sync;

explain shape plan
select g_low, count(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, count(distinct dst_high)
from t_md_hash_id
group by g_low;
```

### T3-F. 聚合函数维度：同一策略判断下覆盖不同 distinct 聚合函数

```text
T3-F. 聚合函数覆盖
├── 正例：满足分布时，不同 distinct 聚合函数都应走 split
│   ├── SQL：sum(distinct id) group by g_low
│   ├── SQL：sum0(distinct id) group by g_low
│   ├── SQL：group_concat(distinct cast(id as string)) group by g_low
│   ├── 条件：id 是分布列，满足分布
│   ├── 预期：都触发新逻辑，走 split
│   └── 断言：不出现对应的 multi_distinct_sum / multi_distinct_sum0 / multi_distinct_group_concat
│
└── 反例：不满足分布时，不同 distinct 聚合函数都应回到旧逻辑
    ├── SQL：sum(distinct dst_high) group by g_low
    ├── SQL：sum0(distinct dst_high) group by g_low
    ├── SQL：group_concat(distinct cast(dst_high as string)) group by g_low
    ├── 条件：dst_high 不是分布列
    ├── 预期：不能触发新逻辑，回到旧策略
    └── 断言：若旧逻辑应使用 multi_distinct 族函数，则出现对应的 multi_distinct_* 形式
```

SQL 示例：

```sql
analyze table t_md_hash_id with sync;

explain shape plan
select g_low, sum(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum0(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, group_concat(distinct cast(id as string))
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum(distinct dst_high)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum0(distinct dst_high)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, group_concat(distinct cast(dst_high as string))
from t_md_hash_id
group by g_low;
```

### T4. 原始列可以命中，表达式不能命中

```text
T4. SlotReference 约束
├── 正例：distinct 参数是原始分布列
│   ├── SQL：count(distinct id) group by g_low
│   ├── 条件：id 是原始 SlotReference，且是分布列
│   ├── 预期：触发新逻辑，走 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：distinct 参数是分布列上的表达式
    ├── SQL：count(distinct id + 1) group by g_low
    ├── SQL：count(distinct cast(id as bigint)) group by g_low
    ├── 条件：表达式不是原始 SlotReference
    ├── 预期：不能触发新逻辑，回到旧策略
    └── 断言：不能仅因为表达式里包含 id 就强制 split
```

SQL 示例：

```sql
explain shape plan
select g_low, count(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, count(distinct id + 1)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, count(distinct cast(id as bigint))
from t_md_hash_id
group by g_low;
```

### T5. Project alias 可以回溯，Project expression 不能回溯

```text
T5. Project 回溯
├── 正例：alias 指向原始分布列
│   ├── SQL：外层 count(distinct id_alias)
│   ├── 内层：select id as id_alias
│   ├── 预期：id_alias 回溯到 id，触发新逻辑，走 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：alias 指向表达式
    ├── SQL：外层 count(distinct id_alias)
    ├── 内层：select id + 1 as id_alias
    ├── 预期：id_alias 不能回溯为原始分布列，回到旧策略
    └── 断言：不能把表达式 alias 误判为分布列 alias
```

SQL 示例：

```sql
explain shape plan
select g, count(distinct id_alias)
from (
    select id as id_alias, g_low as g
    from t_md_hash_id
) t
group by g;

explain shape plan
select g, count(distinct id_alias)
from (
    select id + 1 as id_alias, g_low as g
    from t_md_hash_id
) t
group by g;
```

### T6. Project / Filter 可以穿透，Join 不能穿透

```text
T6. 中间节点边界
├── 正例：Project + Filter 后仍能回溯到单表分布列
│   ├── SQL：内层 select k1 as k_alias where v > 0
│   ├── 条件：中间节点只有 Project / Filter
│   ├── 预期：触发新逻辑，走 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：Join 后的列不能直接按单表分布判断
    ├── SQL：t1 join t2 后 count(distinct t1.k1)
    ├── 条件：聚合 child 不是单个 LogicalOlapScan，也不是 Project / Filter 链
    ├── 预期：不能触发新逻辑，回到旧策略
    └── 断言：不能仅因为 t1.k1 是底表分布列就强制 split
```

SQL 示例：

```sql
explain shape plan
select g, count(distinct k_alias)
from (
    select k1 as k_alias, g
    from t_md_lineage
    where v > 0
) t
group by g;

explain shape plan
select t1.g, count(distinct t1.k1)
from t_md_lineage t1
join t_md_lineage t2 on t1.k2 = t2.k2
group by t1.g;
```

### T7. 单分布列完整覆盖可以命中，多分布列部分覆盖不能命中

```text
T7. 分布列覆盖范围
├── 正例：单分布列表，distinct 覆盖全部分布列
│   ├── 表：distributed by hash(id)
│   ├── SQL：count(distinct id)
│   ├── 预期：触发新逻辑，走 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：多分布列表，distinct 只覆盖部分分布列
    ├── 表：distributed by hash(a, b)
    ├── SQL：count(distinct a)
    ├── SQL：count(distinct b)
    ├── 预期：不能触发新逻辑，回到旧策略
    └── 断言：必须覆盖全部分布列，不能只覆盖其中一列
```

SQL 示例：

```sql
explain shape plan
select g_low, count(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g, count(distinct a)
from t_md_hash_ab
group by g;

explain shape plan
select g, count(distinct b)
from t_md_hash_ab
group by g;
```

### T8. 单列 distinct 新逻辑和多列 count distinct 前置分支不能混淆

```text
T8. 前置分支隔离
├── 正例：单列 distinct 分布命中
│   ├── SQL：count(distinct id)
│   ├── 条件：id 是单列分布列
│   ├── 预期：由新分布逻辑触发 split
│   └── 断言：不出现 multi_distinct_count
│
└── 反例：count(distinct a, b) 即使看起来覆盖 hash(a, b)，也不是新逻辑证明点
    ├── SQL：count(distinct a, b)
    ├── 条件：代码前置分支本来就禁止多列 count distinct 走 multi_distinct
    ├── 预期：保持原有多列 distinct 处理逻辑
    └── 断言：不能用该 case 证明“满足分布直接 split”生效
```

SQL 示例：

```sql
explain shape plan
select g_low, count(distinct id)
from t_md_hash_id
group by g_low;

explain shape plan
select g, count(distinct a, b)
from t_md_hash_ab
group by g;
```

### T9. 聚合函数与普通聚合混合时，函数级别不能丢失

```text
T9. distinct 聚合函数混合普通聚合
├── 正例：满足分布 + distinct 聚合函数 + 普通聚合
│   ├── SQL：count(distinct id), sum(payload), max(payload)
│   ├── SQL：sum(distinct id), sum(payload), count(payload)
│   ├── SQL：sum0(distinct id), max(payload), count(payload)
│   ├── 条件：id 是分布列
│   ├── 预期：整体走 split，且 distinct 聚合函数结果正确
│   └── 断言：不出现 multi_distinct_*，普通聚合结果不变
│
└── 反例：不满足分布 + distinct 聚合函数 + 普通聚合
    ├── SQL：count(distinct dst_high), sum(payload), max(payload)
    ├── SQL：sum(distinct dst_high), sum(payload), count(payload)
    ├── SQL：sum0(distinct dst_high), max(payload), count(payload)
    ├── 条件：dst_high 不是分布列
    ├── 预期：不能触发新逻辑，回到旧策略
    └── 断言：若旧逻辑应走 multi_distinct，则出现对应 multi_distinct_*；普通聚合结果不变
```

SQL 示例：

```sql
explain shape plan
select g_low, count(distinct id), sum(payload), max(payload)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum(distinct id), sum(payload), count(payload)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum0(distinct id), max(payload), count(payload)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, count(distinct dst_high), sum(payload), max(payload)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum(distinct dst_high), sum(payload), count(payload)
from t_md_hash_id
group by g_low;

explain shape plan
select g_low, sum0(distinct dst_high), max(payload), count(payload)
from t_md_hash_id
group by g_low;
```

## 4. 兼容性补充

这些不是主测试点，不适合作为“满足分布”的证明，只用于确认本次改动没有破坏原路径。

```text
兼容性补充
├── session 强制策略
│   ├── agg phase = 1 / 2
│   └── 预期：按原 session 强制策略处理
│
├── 无 group by 的 scalar aggregate
│   ├── SQL：select count(distinct id) from t_md_hash_id
│   └── 预期：不被有 group by 的 split 规则误伤
│
└── 特殊聚合函数
    ├── group_concat(distinct ...) 的特殊参数形式
    ├── order by / separator 组合
    ├── sum(distinct ...) 的类型边界
    ├── sum0(distinct ...) 的类型边界
    └── 预期：在主逻辑已覆盖函数级策略后，这里只补特殊语法和类型边界
```

## 5. 验证口径

```text
验证口径
├── 正例断言
│   ├── 必须走 split strategy
│   ├── 不出现 multi_distinct_count
│   ├── 不出现 multi_distinct_sum
│   ├── 不出现 multi_distinct_sum0
│   ├── 不出现 multi_distinct_group_concat
│   └── 结果正确
│
├── 反例断言
│   ├── 不能触发“满足分布直接 split”
│   ├── 必须回到旧策略
│   ├── 若旧策略应走 multi_distinct，则必须出现对应的 multi_distinct_*
│   └── 若旧策略应走 split，则原因必须是旧策略，而不是分布命中
│
└── 对照断言
    ├── 正反例只改变一个关键条件
    ├── 其他数据分布和查询形态尽量保持一致
    └── 通过 plan 差异确认判断点生效
```

## 6. 优先级

```text
P0
├── T1：判断对象必须是 distinct key，不是 group by key
├── T2：满足分布时，统计信息不能改变 split 结论
├── T3：满足分布时，NDV 组合不能改变 split 结论
├── T3-F：同一策略判断下覆盖 count / sum / sum0 / group_concat
├── T5：Project alias 可以回溯，Project expression 不能回溯
├── T6：Project / Filter 可以穿透，Join 不能穿透
└── T9：distinct 聚合函数和普通聚合混合

P1
├── T4：原始列可以命中，表达式不能命中
├── T7：单分布列完整覆盖可以命中，多分布列部分覆盖不能命中
└── T8：单列 distinct 新逻辑和多列 count distinct 前置分支不能混淆

P2
├── T10：满足分布场景性能收益
├── T11：不满足分布场景性能无回归
└── 兼容性补充：session 强制策略、scalar aggregate、特殊聚合函数
```

## 7. 风险树

```text
主要风险
├── 正例漏命中
│   ├── 满足分布但被 unknown stats 分支覆盖
│   ├── 满足分布但被 NDV 旧逻辑覆盖
│   ├── Project / Filter 后没有正确回溯
│   └── 只覆盖了 count(distinct) 而漏了 sum/sum0/group_concat
│
├── 反例误命中
│   ├── group by key 命中分布被误认为 distinct key 命中
│   ├── 表达式被误认为原始分布列
│   ├── 表达式 alias 被误认为原始列 alias
│   ├── Join 后仍按底表分布短路
│   └── 多分布列只覆盖一列也被误判为完整覆盖
│
└── 分支混淆
    ├── count(distinct a, b) 被误当作新分布逻辑证明点
    ├── 特殊聚合函数完全被放到兼容性，导致主逻辑漏测函数维度
    └── 混合聚合场景只测 plan 不测结果
```

## 8. 性能测试方案

性能测试也按“同一个测试点设计正反例”的方式组织，不单独做松散的 benchmark 清单。

核心思路：

```text
性能测试主轴
├── 正例：满足分布，新增 split 选择应带来收益或至少不劣化
└── 反例：不满足分布，性能行为应保持旧策略特征，不出现异常回退
```

### 8.1 性能环境要求

```text
性能环境
├── 建议至少 3 BE，最好 3~5 BE
├── 关闭自动 analyze 干扰
├── 固定并发度和 session 变量
├── 使用稳定的大数据量
└── 每条 SQL 至少预热 3 次，正式测 5~10 次取中位数
```

建议固定的观测项：

- 总耗时
- Peak Memory
- Exchange / Shuffle Bytes
- HashAgg 输入行数与输出行数
- BE CPU 时间
- Profile 中 Aggregation / Exchange 节点耗时占比

推荐固定的 session 设置：

```sql
set enable_nereids_planner=true;
set enable_fallback_to_original_planner=false;
set enable_parallel_result_sink=false;
set runtime_filter_mode=OFF;
set enable_auto_analyze=false;
set parallel_pipeline_task_num=8;
set be_number_for_test=3;
```

说明：

- `parallel_pipeline_task_num` 需要根据测试集群核数统一固定
- 如果测试环境 BE 数量不止 3 台，`be_number_for_test` 也应固定成真实使用值
- 功能测试和性能测试不要混用一套 session，性能测试应尽量稳定、少变量

### 8.2 性能数据树

```text
性能数据
├── t_md_perf_hash_id
│   ├── 分布：distributed by hash(id)
│   ├── id：高 NDV，分布列
│   ├── g_low：低 NDV
│   ├── g_high：高 NDV
│   ├── dst_high：高 NDV，非分布列
│   └── payload：普通聚合列
│
└── 数据规模建议
    ├── 小规模：1000 万行
    ├── 中规模：5000 万行
    ├── 大规模：1 亿行
    └── 保证 g_low 低基数、id / dst_high 高基数特征稳定
```

#### 8.2.1 性能表设计

建议单独建一张性能表，不复用功能测试小表。

表设计目标：

- `id` 是高基数分布列，用来构造“满足分布”的正例
- `dst_high` 是高基数非分布列，用来构造“不满足分布”的反例
- `g_low` 是低基数 group by 列，用来逼近旧逻辑更偏向 `multi_distinct` 的场景
- `g_high` 是高基数 group by 列，用来补充高基数 group by 观察
- `payload` 用于混合聚合性能验证

建表 SQL：

```sql
drop table if exists t_md_perf_hash_id;
create table t_md_perf_hash_id (
    id bigint,
    g_low int,
    g_high bigint,
    dst_high bigint,
    payload bigint,
    padding varchar(32)
)
duplicate key(id)
distributed by hash(id)
buckets 32
properties(
    "replication_num" = "1"
);
```

字段设计说明：

- `id`
  - 高 NDV
  - 分布列
  - 用于 `count(distinct id)` / `sum(distinct id)` / `sum0(distinct id)` 正例
- `g_low`
  - 低 NDV
  - 推荐 10 或 100 个取值
  - 用于构造 `group by` 低基数场景
- `g_high`
  - 高 NDV
  - 推荐接近行数级别
  - 用于高基数 group by 性能观察
- `dst_high`
  - 高 NDV
  - 非分布列
  - 用于功能和性能反例
- `payload`
  - 普通聚合列
  - 用于混合聚合观察
- `padding`
  - 可选
  - 用于适度放大行宽，观察 exchange / agg 的差异

#### 8.2.2 造数方案

建议优先使用 `numbers` 表函数造数，这和现有回归里常见写法一致。

小规模 1000 万行：

```sql
insert into t_md_perf_hash_id
select
    number as id,
    number % 10 as g_low,
    number as g_high,
    (number * 17) % 10000000 as dst_high,
    number % 1000 as payload,
    lpad(cast(number % 1000 as string), 32, '0') as padding
from numbers("number" = "10000000");
```

中规模 5000 万行：

```sql
insert into t_md_perf_hash_id
select
    number as id,
    number % 10 as g_low,
    number as g_high,
    (number * 17) % 50000000 as dst_high,
    number % 1000 as payload,
    lpad(cast(number % 1000 as string), 32, '0') as padding
from numbers("number" = "50000000");
```

大规模 1 亿行：

```sql
insert into t_md_perf_hash_id
select
    number as id,
    number % 10 as g_low,
    number as g_high,
    (number * 17) % 100000000 as dst_high,
    number % 1000 as payload,
    lpad(cast(number % 1000 as string), 32, '0') as padding
from numbers("number" = "100000000");
```

造数后执行：

```sql
analyze table t_md_perf_hash_id with sync;
```

造数说明：

- `g_low = number % 10`
  - 保证 `group by` 基数很低
- `id = number`
  - 保证 `count(distinct id)` / `sum(distinct id)` 基数极高
- `dst_high = (number * 17) % N`
  - 保持高基数，但不是分布列
  - 用于“不满足分布”的反例
- `payload = number % 1000`
  - 普通聚合列，值分布稳定

如果环境资源一般，建议先跑 1000 万和 5000 万两档；如果资源较充足，再加 1 亿行。

#### 8.2.3 跑数前检查

在正式压测前，先做三件事：

1. 确认计划形态

```sql
explain shape plan
select g_low, count(distinct id)
from t_md_perf_hash_id
group by g_low;
```

2. 确认反例仍走旧策略

```sql
explain shape plan
select g_low, count(distinct dst_high)
from t_md_perf_hash_id
group by g_low;
```

3. 确认统计信息已生效

```sql
show column stats t_md_perf_hash_id;
```

### T10. 满足分布场景的性能收益

```text
T10. 满足分布性能收益
├── 正例：满足分布 + 旧逻辑本可能倾向 multi_distinct
│   ├── SQL：count(distinct id) group by g_low
│   ├── SQL：sum(distinct id) group by g_low
│   ├── SQL：sum0(distinct id) group by g_low
│   ├── 条件：id 是分布列，g_low 低 NDV，id 高 NDV
│   ├── 预期：变更后走 split
│   ├── 性能预期：相较变更前或相较强制 multi_distinct，不劣化且通常更优
│   └── 重点观察：Exchange / Shuffle、HashAgg 压力、Peak Memory
│
└── 反例：同样 NDV 组合，但不满足分布
    ├── SQL：count(distinct dst_high) group by g_low
    ├── SQL：sum(distinct dst_high) group by g_low
    ├── SQL：sum0(distinct dst_high) group by g_low
    ├── 条件：dst_high 高 NDV 但不是分布列
    ├── 预期：回到旧逻辑，不应因为本次改动出现异常性能波动
    └── 重点观察：性能行为与旧策略一致
```

说明：

- 这组是本次性能验证的主证明点
- 正反例只改 `distinct key` 是否满足分布，其他形态尽量保持一致

#### 8.2.4 T10 具体查询清单

推荐至少跑以下 6 条 SQL。

正例组：满足分布

```sql
select g_low, count(distinct id)
from t_md_perf_hash_id
group by g_low;

select g_low, sum(distinct id)
from t_md_perf_hash_id
group by g_low;

select g_low, sum0(distinct id)
from t_md_perf_hash_id
group by g_low;
```

反例组：不满足分布

```sql
select g_low, count(distinct dst_high)
from t_md_perf_hash_id
group by g_low;

select g_low, sum(distinct dst_high)
from t_md_perf_hash_id
group by g_low;

select g_low, sum0(distinct dst_high)
from t_md_perf_hash_id
group by g_low;
```

这 6 条 SQL 的设计意图：

- `g_low` 一致
- 行数一致
- 只改变 `distinct key` 是否满足分布
- 便于把性能差异归因到“新分布判断是否命中”

#### 8.2.5 T10 执行方式

每条 SQL 建议按以下流程跑：

```text
单条 SQL 跑法
├── explain shape plan：确认策略
├── 预热 3 次：不记结果
├── 正式执行 5~10 次：记录耗时
├── 抽 1 次开 profile：记录 Peak Memory / Exchange / Agg 指标
└── 取中位数作为该 SQL 最终结果
```

建议记录表头：

```text
SQL 名称 | 数据规模 | 版本 | 中位耗时 | P95 耗时 | Peak Memory | Shuffle Bytes | 备注
```

### T11. 满足分布与不满足分布在混合聚合下的性能对照

```text
T11. 混合聚合性能对照
├── 正例：满足分布 + distinct 聚合 + 普通聚合
│   ├── SQL：count(distinct id), sum(payload), max(payload) group by g_low
│   ├── SQL：sum(distinct id), sum(payload), count(payload) group by g_low
│   ├── 条件：id 是分布列
│   ├── 预期：走 split，整体性能不劣化
│   └── 重点观察：普通聚合是否因改写被拖慢
│
└── 反例：不满足分布 + distinct 聚合 + 普通聚合
    ├── SQL：count(distinct dst_high), sum(payload), max(payload) group by g_low
    ├── SQL：sum(distinct dst_high), sum(payload), count(payload) group by g_low
    ├── 条件：dst_high 不是分布列
    ├── 预期：继续走旧策略
    └── 重点观察：本次改动不能引入额外性能回退
```

#### 8.2.6 T11 具体查询清单

正例组：满足分布 + distinct 聚合 + 普通聚合

```sql
select g_low, count(distinct id), sum(payload), max(payload)
from t_md_perf_hash_id
group by g_low;

select g_low, sum(distinct id), sum(payload), count(payload)
from t_md_perf_hash_id
group by g_low;

select g_low, sum0(distinct id), max(payload), count(payload)
from t_md_perf_hash_id
group by g_low;
```

反例组：不满足分布 + distinct 聚合 + 普通聚合

```sql
select g_low, count(distinct dst_high), sum(payload), max(payload)
from t_md_perf_hash_id
group by g_low;

select g_low, sum(distinct dst_high), sum(payload), count(payload)
from t_md_perf_hash_id
group by g_low;

select g_low, sum0(distinct dst_high), max(payload), count(payload)
from t_md_perf_hash_id
group by g_low;
```

这一组主要回答：

- 满足分布改走 split 后，`sum(payload)` / `max(payload)` / `count(payload)` 是否被拖慢
- 反例组是否维持旧策略的性能形态

#### 8.2.7 可选补充查询

如果环境允许，可以加两类补充：

1. 高基数 group by 补充观察

```sql
select g_high, count(distinct id)
from t_md_perf_hash_id
group by g_high;

select g_high, count(distinct dst_high)
from t_md_perf_hash_id
group by g_high;
```

2. `group_concat(distinct ...)` 补充观察

```sql
select g_low, group_concat(distinct cast(id as string))
from t_md_perf_hash_id
group by g_low;

select g_low, group_concat(distinct cast(dst_high as string))
from t_md_perf_hash_id
group by g_low;
```

说明：

- `group_concat(distinct ...)` 更适合作为补充项，因为结果字符串较大、噪声更高
- 若环境不稳定，这类查询可以只做 profile 观察，不作为强验收项

### 8.3 性能对照方式

```text
性能对照方式
├── 对照 1：同一 SQL，变更前 vs 变更后
│   └── 最能直接证明本次改动收益
│
├── 对照 2：同一数据分布下，满足分布 SQL vs 不满足分布 SQL
│   └── 用于证明“只有命中新逻辑时才出现新的性能形态”
│
└── 对照 3：同一 SQL 的 explain / profile 对照
    ├── plan 形态差异
    ├── exchange 节点开销差异
    └── agg 节点开销差异
```

建议优先做的对照矩阵：

```text
对照矩阵
├── 版本对照
│   ├── 变更前：old branch / baseline
│   └── 变更后：current branch
│
├── 查询对照
│   ├── 正例：distinct id
│   └── 反例：distinct dst_high
│
└── 规模对照
    ├── 1000 万
    ├── 5000 万
    └── 1 亿
```

如果时间有限，最小可执行组合建议是：

- `1000 万` 和 `5000 万`
- `count(distinct id) group by g_low`
- `count(distinct dst_high) group by g_low`
- `count(distinct id), sum(payload), max(payload) group by g_low`
- `count(distinct dst_high), sum(payload), max(payload) group by g_low`

### 8.4 性能验收口径

```text
性能验收
├── 满足分布场景
│   ├── 允许持平
│   ├── 更理想的是耗时和内存下降
│   └── 至少不能出现明显退化
│
└── 不满足分布场景
    ├── 性能形态应与旧策略一致
    └── 不能因为新增判断带来额外回归
```

建议采用简单门槛：

- 满足分布场景：
  - 中位耗时不劣于基线
  - Peak Memory 不明显高于基线
  - Shuffle / Exchange 开销不明显恶化
- 不满足分布场景：
  - 中位耗时波动控制在合理范围内
  - Profile 主热点与旧版本一致

更具体一点可以写成：

```text
建议验收门槛
├── 满足分布场景
│   ├── 中位耗时 <= 基线 * 1.05
│   ├── Peak Memory <= 基线 * 1.10
│   └── 若耗时下降或内存下降，则记录为收益
│
└── 不满足分布场景
    ├── 中位耗时 <= 基线 * 1.10
    ├── Peak Memory <= 基线 * 1.10
    └── Profile 热点节点类型不应异常变化
```

说明：

- 这里门槛是建议值，最终可以根据你们集群波动情况调整
- 如果环境波动较大，可重点比较中位数和 profile 形态，不强求极小波动

### 8.5 性能风险树

```text
性能风险
├── 满足分布场景没有收益
│   ├── plan 虽然变了，但 exchange / agg 压力没降
│   └── split 额外阶段带来新的开销
│
├── 满足分布场景反而退化
│   ├── 普通聚合混合时被拖慢
│   └── 某些 distinct 聚合函数收益不一致
│
└── 不满足分布场景被误伤
    ├── 新增判断带来额外分析开销
    └── 旧策略路径性能形态被意外改变
```
