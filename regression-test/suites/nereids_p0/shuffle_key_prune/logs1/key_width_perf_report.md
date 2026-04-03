# Agg Shuffle Key Prune — Key Width 性能实验报告

> **执行时间**：2026-03-25 14:14 ~ 14:17（约 3 分钟）
> **集群配置**：3 BE（172.20.56.66/67/68/69），8 pipeline task
> **数据量**：1 亿行，变列数 GROUP BY + 1 列 SUM
> **测试文件**：`agg_shuffle_key_prune_perf_key_width_heavy_manual.groovy`
> **测试结果**：✅ **PASSED**（1 suite，0 failed）

## 实验设计

每个 Case 的 GROUP BY 列数不同，OFF 基线的 shuffle 列数也不同；ON 始终裁剪到 `good_key` 一列。测量"省掉 N 列 hash"带来的收益。

| Case | GROUP BY 列数 | OFF（Shuffle 列数） | ON（裁到） | 验证 |
|------|-------------|-------------------|-----------|------|
| KW00 | 1 列 (good_key) | 1 | 1（无裁剪，基线） | planChanged=false |
| KW01 | 6 列 (good_key + k2~k6) | 6 → 1 | assertSelectedKey ✅ | planChanged=true |
| KW02 | 4 列 (good_key + k2~k4) | 4 → 1 | assertSelectedKey ✅ | planChanged=true |
| KW03 | 2 列 (good_key + k2) | 2 → 1 | assertSelectedKey ✅ | planChanged=true |

## 性能数据

### 原始数据（5 次测量，单位 ms）

| Case | 裁剪幅度 | OFF | ON |
|------|---------|-----|-----|
| KW00 | 1→1（无裁剪） | 542, 556, **558**, 565, 582 | 554, 556, **567**, 577, 601 |
| KW01 | 6→1（省 5 列） | 2954, 2984, **2987**, 2990, 3196 | 2898, 2912, **2929**, 2943, 2974 |
| KW02 | 4→1（省 3 列） | 2507, 2526, **2530**, 2556, 2590 | 2458, 2488, **2501**, 2552, 2603 |
| KW03 | 2→1（省 1 列） | 1614, 1624, **1638**, 1640, 1642 | 1610, 1625, **1640**, 1649, 1673 |

> 加粗为中位数

### 汇总

| Case | 裁剪幅度 | OFF 中位 | ON 中位 | **Speedup** | 节省 |
|------|---------|---------|--------|------------|------|
| KW00 | 无裁剪 | 558ms | 567ms | **0.98x** | -9ms（噪声） |
| KW01 | 6→1 | 2987ms | 2929ms | **1.02x** | 58ms |
| KW02 | 4→1 | 2530ms | 2501ms | **1.01x** | 29ms |
| KW03 | 2→1 | 1638ms | 1640ms | **1.00x** | -2ms（噪声） |

## 关键结论

### 1. Plan 确定性验证 ✅
- KW00 无裁剪（`planChanged=false`），符合预期
- KW01/KW02/KW03 均裁到 `good_key`（`assertSelectedKey` 通过）
- 结果一致性验证全部通过

### 2. Key Width 收益极为微弱
在本次测试条件下（1 亿行，全 bigint 列），Speedup 仅 **0.98x~1.02x**，基本在测量噪声范围内。

### 3. 原因分析

> [!IMPORTANT]
> **收益微弱不代表功能无用**，而是因为本实验的特殊设计：

| 因素 | 影响 |
|------|------|
| **全 bigint 列** | bigint hash 单列成本极低（~8 字节），省 5 列 bigint hash 节省的绝对时间很少 |
| **GROUP BY 无数据倾斜** | 1 亿行均匀分布到 800 万组，shuffle 数据量相同 → hash 列数差异占比小 |
| **总执行时间主要由 scan/agg 占据** | hash 计算仅占整体 query time 的很小比例 |

### 4. 与 Datatype 实验的对比

| 实验 | 变量 | 最大 Speedup | 核心收益来源 |
|------|------|-------------|-------------|
| Datatype | 被选中列的类型 | **1.20x** | varchar(128) → bigint 的 hash 单次 cost 差异大 |
| Key Width | 裁掉的列数 | **1.02x** | 全 bigint 场景下，多裁几列只省了几次 8 字节 hash |

> **结论**：Shuffle Key Pruning 的核心收益来自**类型降级**（string→bigint），而非**列数裁剪**。在全 bigint 表上裁剪列数的边际收益极小。

## 建议

1. **Key Width 实验的真正价值**：如果将 k2~k6 改为 `varchar(128)` 或 `string` 类型，裁掉 5 列 string hash 的收益会显著增大
2. **当前结果仍有参考意义**：证明了在"最不利"场景下（全 bigint），Pruning 至少不会引入性能回退（Speedup ≈ 1.0x）
3. **生产环境**：真实业务表通常包含 varchar/string 列，实际收益会介于 Datatype 实验和 Key Width 实验之间
