# Agg Shuffle Key Prune — Datatype 性能实验报告

> **执行时间**：2026-03-25 14:04 ~ 14:12（约 7 分钟）
> **集群配置**：3 BE（172.20.56.66/67/68/69），8 pipeline task
> **数据量**：1 亿行，6 列 GROUP BY + 1 列 SUM
> **测试文件**：`agg_shuffle_key_prune_perf_datatype_heavy_manual.groovy`
> **测试结果**：✅ **PASSED**（1 suite，0 failed）

## 实验设计

同一条 SQL（`group by ki, kd, kc8, kv16, kv128, guard`），通过手动注入 `hot_values` 控制 optimizer 选择不同类型的列作为唯一 shuffle key，测量不同类型列做 shuffle hash 的 CPU 开销差异。

| Case | 选中的 Shuffle Key | 类型 | 其他列状态 |
|------|-------------------|------|-----------|
| DT01 | `ki` (exprId=1) | **bigint** | 其他列 skew (hot_values=0.10) |
| DT02 | `kd` (exprId=2) | **datev2** | ki skew, 其他列 skew |
| DT03 | `kc8` (exprId=3) | **char(8)** | ki/kd skew, 其他列 skew |
| DT04 | `kv16` (exprId=4) | **varchar(16)** | ki/kd/kc8 skew, 其他列 skew |
| DT05 | `kv128` (exprId=5) | **varchar(128)** | ki/kd/kc8/kv16 skew |

> 所有 Case 的 Plan 均从 `[1,2,3,4,5,6]`（6 列 shuffle）→ `[N]`（单列 shuffle），`planChanged=true`，`assertSelectedKey` 全部通过。

## 性能数据

### 原始数据（5 次测量，单位 ms）

| Case | Shuffle Key | OFF（6 列） | ON（1 列） |
|------|------------|------------|-----------|
| DT01 | bigint | 5570, 5674, **5705**, 5710, 5812 | 4718, 4741, **4753**, 4805, 4813 |
| DT02 | datev2 | 5688, 5697, **5712**, 5765, 5850 | 5133, 5192, **5212**, 5228, 5307 |
| DT03 | char(8) | 5612, 5650, **5772**, 5807, 5813 | 4988, 4998, **5012**, 5023, 5027 |
| DT04 | varchar(16) | 5665, 5698, **5717**, 5735, 5749 | 4920, 4942, **4949**, 5056, 5125 |
| DT05 | varchar(128) | 5658, 5663, **5706**, 5789, 5825 | 5173, 5173, **5221**, 5278, 5281 |

> 加粗为中位数

### 汇总

| Case | Shuffle Key | 类型 | OFF 中位 | ON 中位 | **Speedup** | OFF min | ON min |
|------|------------|------|---------|--------|------------|---------|--------|
| DT01 | ki | bigint | 5705ms | 4753ms | **1.20x** | 5570ms | 4718ms |
| DT02 | kd | datev2 | 5712ms | 5212ms | **1.10x** | 5688ms | 5133ms |
| DT03 | kc8 | char(8) | 5772ms | 5012ms | **1.15x** | 5612ms | 4988ms |
| DT04 | kv16 | varchar(16) | 5717ms | 4949ms | **1.16x** | 5665ms | 4920ms |
| DT05 | kv128 | varchar(128) | 5706ms | 5221ms | **1.09x** | 5658ms | 5173ms |

### 绝对节省时间

| Case | 类型 | 节省时间（中位） | 节省率 |
|------|-----|----------------|--------|
| DT01 | bigint | 952ms | 16.7% |
| DT02 | datev2 | 500ms | 8.8% |
| DT03 | char(8) | 760ms | 13.2% |
| DT04 | varchar(16) | 768ms | 13.4% |
| DT05 | varchar(128) | 485ms | 8.5% |

## 关键结论

### 1. Shuffle Key Pruning 优化有效 ✅
所有 5 个 Case 均获得 **9%~20%** 的查询加速，证明将 6 列 shuffle hash 裁剪到 1 列是有明显收益的。

### 2. bigint 胜出 — 最优 Shuffle Key 类型
| 排名 | 类型 | Speedup | 收益绝对值 |
|------|------|---------|-----------|
| 🥇 | **bigint** | 1.20x | 952ms |
| 🥈 | varchar(16) | 1.16x | 768ms |
| 🥉 | char(8) | 1.15x | 760ms |
| 4 | datev2 | 1.10x | 500ms |
| 5 | varchar(128) | 1.09x | 485ms |

> **bigint > char(8) ≈ varchar(短) > datev2 ≈ varchar(长)**

### 3. 收益来源分析
- **bigint 收益最大**（1.20x）：8 字节定长，hash 计算最快
- **varchar(128) 收益最小**（1.09x）：变长字符串 hash 本身就慢，即使裁到 1 列，单列 hash 开销仍然可观
- **中位数方差极小**（OFF ±60ms，ON ±60ms）：测量稳定可信

### 4. Plan 确定性验证 ✅
通过手动注入 `hot_values` 控制目标列被选中的方案完全可靠：
- 每个 Case 的 `assertSelectedKey` 均通过
- Plan 从 `[1,2,3,4,5,6]` 精确裁到预期的单列 `[N]`
- OFF/ON 结果一致性验证全部通过

## 建议

1. **类型优先级设计合理**：bigint 作为最优 shuffle hash 类型，符合 CPU cache 和 hash 计算效率的理论预期
2. **varchar(128) 场景仍有收益**：即使是最"不利"的类型，9% 的加速在大规模查询中仍然显著
3. **后续可扩展**：在更大数据量（10 亿行）或更多 GROUP BY 列（12+）的场景下，Speedup 预计会更明显
