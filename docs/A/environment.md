# 计算环境（报告第 10.3 节）

状态：**已在正式运行机器上测量（2026-10-06）。** 下表由

```bash
python src/record_environment.py --config configs/project.yaml
```

打印，原始记录见 `logs/environment.json`、`logs/pip_freeze.txt`。

| 字段 | 实际值 |
|---|---|
| 平台 / 机器 | Mac16,13 arm64 |
| CPU | Apple M4 (10 logical) |
| 内存 | 16.0 GiB |
| 磁盘（仓库所在卷） | 494 GB total, 138 GB free |
| OS | macOS-15.7.3-arm64-arm-64bit |
| Python / Java / Spark | 3.11.15 / Eclipse Adoptium 17.0.20.1 / 3.5.8 |
| 执行模式 | local[*] |
| Driver 内存 | 6g |
| shuffle 分区 | 64 |
| 时区 | Asia/Singapore |

## 数据规模与耗时（2026-10-06 全量运行）

耗时为墙钟时间；峰值内存为每 5 秒采样的 java + python 进程 RSS 之和，仅作量级参考。

| 步骤 | 命令 | 输入规模 | 耗时 | 峰值内存 |
|---|---|---|---|---|
| 下载 | `python src/download_data.py` | 8 个文件，8,047,044,701 字节，48,538,713 行（见 `data/manifest.csv`） | 421 s | ~60 MB |
| 原始审计 | `python src/data_quality.py --stage raw` | 48,538,713 原始行 | 103 s（加入 `diurnal_jan_apr` 后重跑 117 s） | ~3.3 GB |
| 清洗 | `python src/clean_data.py --write-samples` | 48,538,713 原始行 → 48,538,627 清洗行 | 629 s | ~4.7 GB |
| 质量审计 | `python src/data_quality.py --stage clean` | 48,538,627 清洗行 | 41 s | ~3.4 GB |

磁盘占用：`data/raw` 7.5 GB，`data/interim` 3.1 GB，`data/clean` 455 MB。
