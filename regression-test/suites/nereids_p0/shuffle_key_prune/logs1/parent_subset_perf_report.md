# Agg Shuffle Key Prune — Parent Subset 性能实验报告

> **执行时间**：2026-03-25 14:18 ~ 14:25（约 7 分钟）
> **集群配置**：3 BE（172.20.56.66/67/68/69），8 pipeline task
> **数据量**：1 亿行 fact 表 + 1 亿行 probe 表
> **测试文件**：`agg_shuffle_key_prune_perf_parent_subset_heavy_manual.groovy`
> **测试结果**：✅ **PASSED**（1 suite，0 failed）

## 实验设计

测试 Join Parent 下推缩窄 agg shuffle key 后的性能影响。fact 表 `group by a,b,c,d,e,f`（6 列），通过 join key 子集驱动 shuffle key 缩窄。

| Case | 场景 | Join 条件 | Parent 下推效果 |
|------|------|----------|----------------|
| PS01 | probe join on a,b,c | 3 列 subset，NDV 高 | 6 列→3 列 |
| PS02 | probe join on a | 1 列 subset，NDV 高 | 6 列→1 列 |
| PS03 | probe join on a (NDV≈阈值) | 1 列 subset，NDV 边界 | 劣化观察 |
| PS04 | both-side agg join on a,b | 两侧 agg 都受益 | 两侧各缩窄 |

## 性能数据

### 原始数据（5 次测量，单位 ms）

| Case | OFF | ON |
|------|-----|-----|
| PS01 | 4455, 4517, **4576**, 4672, 4756 | 4882, 4998, **5042**, 5096, 5210 |
| PS02 | 5106, 5116, **5151**, 5157, 5204 | 5087, 5112, **5141**, 5197, 5246 |
| PS03 | 5051, 5069, **5104**, 5107, 5251 | 5090, 5129, **5140**, 5166, 5297 |
| PS04 | 6949, 6967, **7028**, 7028, 7094 | 6904, 6908, **6926**, 7045, 7098 |

> 加粗为中位数

### 汇总

| Case | 场景 | OFF 中位 | ON 中位 | **Speedup** | 节省时间 |
|------|------|---------|--------|------------|---------|
| PS01 | join on 3 cols | 4576ms | 5042ms | **0.91x** ⚠️ | -466ms |
| PS02 | join on 1 col | 5151ms | 5141ms | **1.00x** | 10ms |
| PS03 | join on 1 col (边界NDV) | 5104ms | 5140ms | **0.99x** | -36ms |
| PS04 | both-side agg | 7028ms | 6926ms | **1.01x** | 102ms |

## 关键结论

### 1. Plan 确定性验证 ✅
所有 4 个 Case 均 `planChanged=true`，说明 Parent Subset 逻辑成功触发，结果一致性验证全部通过。

### 2. Parent Subset 性能中性（无显著收益）

| 观察 | 分析 |
|------|------|
| PS01 反而变慢（0.91x） | 3 列 join key 的 shuffle 分布可能更优，缩窄后分布不均 |
| PS02/PS03 持平（1.00x/0.99x） | 全 bigint 列，hash 列数差异产生的收益微乎其微 |
| PS04 略有收益（1.01x） | 两侧 agg 都缩窄，累计效果略有正向 |

### 3. PS01 劣化分析

> [!WARNING]
> PS01 出现了 **0.91x** 的劣化，值得深入分析。

可能原因：
- **数据分布变化**：6 列联合 hash 的分布均匀性 > 3 列 hash，缩窄后部分 instance 的数据更重
- **Join 侧负载倾斜**：probe 表 join on a,b,c 的分布可能本身就有一定倾斜
- **这是 Parent 路径的预期风险**：Parent 要求的 key 不一定是数据分布最优的 key

### 4. 与其他实验的对比

| 实验 | 核心收益来源 | 最大 Speedup | 结论 |
|------|------------|-------------|------|
| **Datatype** | 类型降级 (string→bigint) | **1.20x** | ✅ 显著收益 |
| **Key Width** | 裁剪列数 (全 bigint) | **1.02x** | ⚪ 收益微弱 |
| **Parent Subset** | Parent 缩窄 (全 bigint) | **1.01x** | ⚪ 基本中性，有劣化风险 |

## 建议

1. **PS01 劣化需要关注**：如果 parent 路径在生产环境也出现缩窄后反而更慢的情况，可能需要加入分布质量检测
2. **全 bigint 场景无法体现 parent subset 收益**：如果 fact 表的 group by 列包含 varchar/string 类型，parent 缩窄到 bigint join key 会有更明显的收益
3. **PS03 边界 NDV 未劣化**：勉强通过 isBalanced 的 key 在性能上与正常 key 没有显著差异（0.99x ≈ 中性）
