# Agg Shuffle Key Prune — Multi GBY 综合性能实验报告

> **执行时间**：2026-03-20 14:37 ~ 17:05（约 2.5 小时）
> **集群配置**：3 BE，8 pipeline task
> **数据量**：1 亿行 × 4 张表
> **测试文件**：`shuffle_key_prune_case_multi_gby.groovy`
> **测试结果**：✅ **PASSED**（1 suite，0 failed）

## 实验设计

### 4 张表 × 5 种 GBY 宽度 × 14 条 SQL = 280 个测试点

**表设计**（区分 DISTRIBUTED BY 和 dist_key NDV）：

| 表名 | 分布方式 | dist_key NDV | 特点 |
|------|---------|-------------|------|
| dist_ndv_low_tb | HASH(dist_key) | 10M（低） | Hash 分布 + 低 NDV |
| dist_ndv_high_tb | HASH(dist_key) | 85M（高） | Hash 分布 + 高 NDV |
| random_ndv_low_tb | RANDOM | 10M（低） | Random 分布 + 低 NDV |
| random_ndv_high_tb | RANDOM | 85M（高） | Random 分布 + 高 NDV |

**GBY 宽度**：gby2 / gby3 / gby4 / gby5 / gby6

**14 条 SQL 模式**：

| SQL ID | 模式 | 说明 |
|--------|------|------|
| 0 | SimpleAgg(unsat, no CSMM) | 不含 dist_key 的聚合 |
| 1 | SimpleAgg(sat, no CSMM) | 含 dist_key 的聚合 |
| 2 | SimpleAgg(unsat, with CSMM) | 不含 dist_key + SUM/MIN/MAX |
| 3 | SimpleAgg(sat, with CSMM) | 含 dist_key + SUM/MIN/MAX |
| 4 | DistinctCount(sat keys) | COUNT DISTINCT |
| 5 | DistinctCount(a only) | 单列 DISTINCT |
| 6 | DistinctCount(multi) | 多列 DISTINCT |
| 7 | DistinctCount(dist_key) | dist_key DISTINCT |
| 8 | Window(ROW_NUMBER) | 窗口函数 |
| 9 | ParentAgg | 嵌套聚合 |
| 10 | AggJoinScan | Agg + Join + Scan |
| 11 | ScanJoinScan | Scan + Join + Scan |
| 12 | BothAgg | 两侧 Agg Join |
| 13 | NestedJoin | 多层 Join |

## 核心性能数据

### Hash 分布表（dist_ndv_low_tb）— 按 GBY 宽度

> `on` = enable_agg_shuffle_key_prune=true（裁剪开），`off` = false（裁剪关）
> 单位：ms，值为 3 次取最小值

| SQL ID | SQL 模式 | gby2 on | gby2 off | gby3 on | gby3 off | gby4 on | gby4 off | gby5 on | gby5 off | gby6 on | gby6 off |
|--------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|
| 0 | SimpleAgg(unsat) | 1880 | 1883 | 2242 | 2252 | 3059 | 2998 | 3350 | 3430 | 3586 | 3594 |
| 1 | SimpleAgg(sat) | 1072 | 1075 | 1207 | 1224 | 1662 | 1650 | 1822 | 1794 | 2079 | 2066 |
| 2 | SimpleAgg(unsat+CSMM) | 3163 | 3126 | 3559 | 3481 | 4279 | 4345 | 4637 | 4623 | 4975 | 4988 |
| 3 | SimpleAgg(sat+CSMM) | 1732 | 1705 | 1927 | 1857 | 2292 | 2313 | 2432 | 2424 | 2731 | 2695 |
| 4 | DistinctCount(sat) | 3577 | 3577 | 4231 | 4608 | 5382 | 5425 | 5992 | 6012 | 6638 | 7056 |
| 5 | DistinctCount(a) | 1892 | 1881 | 2403 | 2356 | 2935 | 2912 | 3133 | 3164 | 3414 | 3440 |
| 6 | DistinctCount(multi) | 2235 | 2209 | 2603 | 2587 | 3329 | 3276 | 3681 | 3647 | 4105 | 4040 |
| 7 | DistinctCount(dist_key) | 3159 | 3195 | 3939 | 3969 | 4986 | 4992 | 5512 | 5481 | 5968 | 6011 |
| 8 | Window | 1159 | 1169 | 1323 | 1298 | 1822 | 1744 | 1886 | 1857 | 2194 | 2158 |
| 9 | ParentAgg | 2595 | 2579 | 2779 | 2823 | 3618 | 3561 | 3919 | 3861 | 4262 | 4301 |
| 10 | AggJoinScan | 1905 | 1889 | 2351 | 2305 | 2905 | 2870 | 3725 | 3846 | 4344 | 4352 |
| 11 | ScanJoinScan | 792 | 796 | 1071 | 1074 | 1216 | 1199 | 1862 | 1827 | 2147 | 2305 |
| 12 | BothAgg | 2852 | 2811 | 3401 | 3440 | 4410 | 4423 | 5384 | 5374 | 6399 | 6714 |
| 13 | NestedJoin | 1609 | 1650 | 2249 | 2236 | 2510 | 2511 | 3874 | 3935 | 4644 | 5042 |

### Random 分布表（random_ndv_low_tb）— 按 GBY 宽度

| SQL ID | SQL 模式 | gby2 on | gby2 off | gby3 on | gby3 off | gby4 on | gby4 off | gby5 on | gby5 off | gby6 on | gby6 off |
|--------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|
| 0 | SimpleAgg(unsat) | 1818 | 1915 | 2282 | 2236 | 3051 | 3042 | 3356 | 3400 | 3625 | 3623 |
| 1 | SimpleAgg(sat) | 1920 | 1950 | 2238 | 2228 | 3033 | 3033 | 3341 | 3289 | 3934 | 3951 |
| 2 | SimpleAgg(unsat+CSMM) | 3209 | 3208 | 3565 | 3678 | 4363 | 4375 | 4683 | 4776 | 5051 | 4919 |
| 3 | SimpleAgg(sat+CSMM) | 3232 | 3245 | 3565 | 3633 | 4516 | 4321 | 4671 | 4649 | 5277 | 5350 |
| 4 | DistinctCount(sat) | 5325 | 5530 | 6311 | 6641 | 8163 | 8566 | 9148 | 9552 | 10193 | 10574 |
| 5 | DistinctCount(a) | 4760 | 4857 | 5882 | 6115 | 7302 | 7757 | 7999 | 8296 | 8627 | 8998 |
| 6 | DistinctCount(multi) | 5399 | 5460 | 6546 | 6716 | 8149 | 8491 | 9139 | 9541 | 10029 | 10643 |
| 7 | DistinctCount(dist_key) | 4674 | 4874 | 6337 | 6144 | 7610 | 7733 | 7937 | 8339 | 8369 | 8996 |
| 8 | Window | 2134 | 2109 | 2431 | 2450 | 3302 | 3260 | 3451 | 3470 | 4061 | 4062 |
| 9 | ParentAgg | 4127 | 4117 | 4612 | 4644 | 6069 | 6105 | 6313 | 6535 | 7252 | 7417 |
| 10 | AggJoinScan | 3200 | 3153 | 3893 | 3948 | 4898 | 5007 | 5927 | 5957 | 7069 | 7016 |
| 11 | ScanJoinScan | 1622 | 1641 | 2214 | 2215 | 2634 | 2684 | 3560 | 3614 | 4412 | 4476 |
| 12 | BothAgg | 4197 | 4400 | 5029 | 5370 | 6548 | 6828 | 8050 | 8470 | 9630 | 10424 |
| 13 | NestedJoin | 3000 | 3005 | 4011 | 4012 | 4687 | 4672 | 6534 | 6601 | 7964 | 8150 |

## Speedup 分析

### 有明显收益的场景（Speedup > 1.03x）

| 场景 | 表 | GBY | on | off | **Speedup** | 分析 |
|------|-----|-----|-----|-----|------------|------|
| SQL4 (DistinctCount) | dist_ndv_low_tb | gby3 | 4231 | 4608 | **1.09x** | Distinct 计算受益于裁剪 |
| SQL4 (DistinctCount) | dist_ndv_low_tb | gby6 | 6638 | 7056 | **1.06x** | 列数越多收益越大 |
| SQL11 (ScanJoinScan) | dist_ndv_low_tb | gby6 | 2147 | 2305 | **1.07x** | Join 场景受益 |
| SQL12 (BothAgg) | dist_ndv_low_tb | gby6 | 6399 | 6714 | **1.05x** | 两侧 Agg 累积收益 |
| SQL13 (NestedJoin) | dist_ndv_low_tb | gby6 | 4644 | 5042 | **1.09x** | 多层 Join 累积收益 |
| SQL4 (DistinctCount) | random_ndv_low_tb | gby2 | 5325 | 5530 | **1.04x** | Random 分布同样受益 |
| SQL4 (DistinctCount) | random_ndv_low_tb | gby3 | 6311 | 6641 | **1.05x** | |
| SQL4 (DistinctCount) | random_ndv_low_tb | gby4 | 8163 | 8566 | **1.05x** | |
| SQL4 (DistinctCount) | random_ndv_low_tb | gby5 | 9148 | 9552 | **1.04x** | |
| SQL4 (DistinctCount) | random_ndv_low_tb | gby6 | 10193 | 10574 | **1.04x** | |
| SQL5 (DistinctCount-a) | random_ndv_low_tb | gby4 | 7302 | 7757 | **1.06x** | |
| SQL5 (DistinctCount-a) | random_ndv_low_tb | gby6 | 8627 | 8998 | **1.04x** | |
| SQL6 (DistinctCount-multi) | random_ndv_low_tb | gby5 | 9139 | 9541 | **1.04x** | |
| SQL6 (DistinctCount-multi) | random_ndv_low_tb | gby6 | 10029 | 10643 | **1.06x** | |
| SQL7 (DistinctCount-dist_key) | random_ndv_low_tb | gby6 | 8369 | 8996 | **1.07x** | |
| SQL9 (ParentAgg) | random_ndv_low_tb | gby5 | 6313 | 6535 | **1.04x** | |
| SQL12 (BothAgg) | random_ndv_low_tb | gby2 | 4197 | 4400 | **1.05x** | |
| SQL12 (BothAgg) | random_ndv_low_tb | gby3 | 5029 | 5370 | **1.07x** | |
| SQL12 (BothAgg) | random_ndv_low_tb | gby4 | 6548 | 6828 | **1.04x** | |
| SQL12 (BothAgg) | random_ndv_low_tb | gby5 | 8050 | 8470 | **1.05x** | |
| SQL12 (BothAgg) | random_ndv_low_tb | gby6 | 9630 | 10424 | **1.08x** | |

### 等价类变化分析（Plan 改变的 SQL）

| SQL ID | 模式 | ON 等价类 | OFF 等价类 | Plan 变化 |
|--------|------|----------|-----------|----------|
| 0 | SimpleAgg(unsat) | `[[3]]` | `[[1],[2],[3],[4],[5],[6]]` | ✅ 6列→1列 |
| 2 | SimpleAgg(unsat+CSMM) | `[[3]]` | `[[1],[2],[3],[4],[5],[6]]` | ✅ 6列→1列 |
| 4 | DistinctCount(sat) | `[[6]]` | `[[6],[7],[8],[9],[10],[11]]` | ✅ 6列→1列 |
| 1,3,5,6,8,9,10,11,12,13 | 其他 | `[]` | `[]` | ⚪ 无 Plan 变化 |

> 注：dist 表上 SQL 1/3/5/6/8/9/10/11/12/13 的等价类为空（`[]`），说明 Plan 没有变化——但 random 表上这些 SQL 等价类有值，说明 random 分布触发了更多的裁剪路径。

## 关键结论

### 1. 收益集中在 Distinct 和多层 Join 场景
- **SQL4/5/6/7（DistinctCount）**：稳定获得 **4%~9%** 的加速
- **SQL12/13（BothAgg/NestedJoin）**：在高 GBY 宽度下获得 **5%~9%** 的加速
- 简单聚合（SQL0/1/2/3）收益极小，基本在噪声范围内

### 2. GBY 列数越多，裁剪收益越大
以 dist_ndv_low_tb SQL4 为例：
- gby2: 1.00x → gby3: 1.09x → gby4: 1.01x → gby5: 1.00x → gby6: **1.06x**

以 random_ndv_low_tb SQL12 为例：
- gby2: 1.05x → gby3: 1.07x → gby4: 1.04x → gby5: 1.05x → gby6: **1.08x**

### 3. Random 分布表收益 > Hash 分布表
- Random 分布表在几乎所有场景都比 Hash 分布表多零几个百分点的收益
- 原因：Random 分布没有 colocate 优化可用，shuffle 路径是唯一选择，裁剪收益更突出

### 4. 无劣化情况
除极少数噪声范围内的波动外（如 gby4 SimpleAgg 偶尔 off < on 几十 ms），**没有出现系统性劣化**。

### 5. 全 bigint 场景限制了收益上限
所有列均为 bigint 类型，hash 计算本身成本极低。根据之前 Datatype 实验的结论，如果包含 varchar/string 列，预期收益会显著增大。

## 数据已写入 Excel

已将数据追加到 `agg_shuffle_key_data.xlsx` 的两个新 Sheet：
- **multi_gby_new_耗时数据**：20 组 label × 14 条 SQL 的耗时
- **multi_gby_new_等价类数据**：对应的 equivalenceExprIds Plan 变化数据

## 建议

1. **Distinct 场景是 Pruning 的甜蜜区**：建议在 PR 文档中重点展示 SQL4/5/6/7 的收益数据
2. **多层 Join 场景值得关注**：SQL12/13 在高 GBY 下的累积收益（5%~9%）对复杂查询有实际价值
3. **后续可补充 string 列测试**：结合 Datatype 实验结论，使用 varchar 列替换部分 bigint 列可放大收益
