# NUS DSA5208 — Project 2

使用 Apache Spark 分析新加坡 2017—2024 年历史降雨，并估计监测站未来 30 分钟累计降雨超过给定阈值的概率。

**当前阶段（2026-10-12）：** A 全量数据、B Task 1、C MLlib 模型均已完成。B 已独立复算 12 组真实预测、补充 v2 事后评估、重载三例真实接口并完成含三位成员姓名学号的全文 PDF 候选版。原冻结 v1 serving 六阈值平均 Brier=0.019248；v2 的同一已查看测试样本补充成绩单独记录，不能混称未触碰测试。尚需成员人工核对、确认接口适用范围及 Canvas 正式提交。

- [完整 PDF](reports/DSA5208_Project2_Report.pdf) / [可编辑报告](reports/GROUP_REPORT.md)
- [B 真实模型复核](reports/b_model_review/B_MODEL_RESULTS.md) / [官方要求逐项对照](docs/B/OFFICIAL_COMPLETION.md)
- 当前 v2 支持 q∈[0,10] mm，历史时间及已知站点；不声称任意阈值、实时服务或暴雨预警性能。

## 课程要求

- **Task 1：** 探索年内天气模式及 2017—2024 年之间的变化，使用图表解释发现。
- **Task 2：** 使用 Spark MLlib 开发和评估概率模型，显式输入为站点、预测时间和降雨量阈值。
- **数据：** [Historical Rainfall across Singapore](https://data.gov.sg/collections/2279/view)，只使用 2017—2024 八个年度数据集。
- **提交：** 完整代码、运行 README、已训练模型、PDF 报告；报告包含所有成员姓名和学号、计算环境、方法、结果、局限及 AI 使用声明。
- **截止：** 2026-10-18；准确时刻和提交限制以 Canvas 为准。

## 团队分工

| 成员 | 主责 |
|---|---|
| A | 数据获取、清洗、质量检查、环境与集成 |
| B | 探索分析、可视化、独立结果核验与报告 |
| C | 标签、特征、MLlib 模型、评估、模型保存与预测接口 |

报告成员：Yikun Zhao（A0353894M）、Zhou Jiahao（A0354090J）、Chen Zheke（A0356140M）。按用户提供的拼写记录；不推断姓名与 A/B/C 的对应关系。

## 目录结构

```text
project2/
├── README.md
├── requirements.txt
├── .gitignore
├── configs/
├── data/
│   ├── raw/
│   ├── clean/
│   ├── features/
│   └── samples/
├── src/
├── tests/
├── models/
│   └── final/
├── reports/
│   ├── tables/
│   └── figures/
├── docs/
│   ├── A/
│   └── C/
└── logs/
```

原始数据、处理后的全量数据、模型二进制、日志、虚拟环境不进入普通 Git；后续课程提交仍需单独包含完整训练模型。小型测试样本、汇总表和图表可按需提交。

## 统一环境基线

下表是团队约定，不代表本机已经安装或完成运行测试。

| 项目 | 约定 |
|---|---|
| 操作系统 | macOS 或 Linux；Windows 建议使用 WSL2 Ubuntu |
| Python | CPython **3.11.x**；统一使用项目内 `.venv`；正式实验记录实际补丁版本 |
| Java | **JDK 17 LTS**，设置 `JAVA_HOME`；正式实验记录发行商和补丁版本 |
| Spark / PySpark | **3.5.8**；使用 `pyspark.sql` 和 DataFrame 版 `pyspark.ml` |
| NumPy | **1.26.4**，与本项目 Spark 3.5 系列保持兼容 |
| Pandas / PyArrow | **2.2.3 / 12.0.1**；主要用于小型聚合表及 Arrow 交互 |
| 时区 | Spark 会话显式设置 `Asia/Singapore` |
| 随机种子 | 默认 **42**；所有支持 seed 的模型/抽样步骤显式指定 |
| 起步资源 | 本地 4 核 / 16 GB RAM；全量处理建议 8 核 / 32 GB RAM、100 GB 以上可用 SSD；最终据试跑调整 |
| 开发执行模式 | `local[4]`；小型烟雾测试用 `local[2]`；实际性能配置后续写入 `configs/` |
| 数据处理与建模 | 全量 ETL、聚合、特征、训练、评估使用 Spark；Pandas/NumPy 不替代全量 Spark 流程 |

`requirements.txt` 管理 Python 包；**它不能安装或强制检查 Python 解释器、JDK、硬件和 Spark 会话配置**，这些由本节统一约定并在环境检查中核验。Spark 本地运行所需组件由 PyPI 的 PySpark 包提供，无需另装一套不同版本的系统 Spark；若连接集群，集群与客户端版本必须一致。

Spark 3.5.8 是对前期计划中 3.5.7 的补丁版本更新。版本选择及兼容性依据：[Spark 3.5.8 官方说明](https://spark.apache.org/docs/3.5.8/)、[PySpark 安装与依赖说明](https://spark.apache.org/docs/3.5.8/api/python/getting_started/install.html)。

### 1. 准备 Python 3.11 与 JDK 17

先安装对应版本的 CPython 和 JDK 17。macOS 若已使用 Homebrew，可执行：

```bash
brew install python@3.11 openjdk@17
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
python3.11 --version
java -version
```

如果使用已被 macOS 注册的 JDK 17，可以用 `export JAVA_HOME="$(/usr/libexec/java_home -v 17)"` 选择它。Linux/WSL2 将 `JAVA_HOME` 设置为实际安装的 JDK 17 路径；不要复制 macOS 的路径。以上环境变量只作用于当前终端，重新打开终端后需重新设置。

### 2. 创建虚拟环境并安装依赖

在仓库根目录执行；本地初始化位置为 `/Users/yikun_zhao/Documents/DSA5208/project2`，其他成员使用自己的克隆目录。

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip check
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"
```

不要把 `.venv` 提交到 Git。不要让旧的 `SPARK_HOME` 或 `PYTHONPATH` 指向另一套 Spark。macOS Apple Silicon 使用原生 arm64 Python 和 JDK，避免与 Rosetta 的 x86_64 安装混用。机器只装有 Python 3.9 或没有 Java 时，必须先补齐本节所列运行时。

### 3. 环境烟雾测试

以下命令只验证 Python、JVM、Spark SQL、MLlib 训练和模型重载，不下载课程数据，也不在仓库子目录生成业务文件。请在依赖安装完成后执行。

```bash
python - <<'PY'
import sys
import tempfile
from pathlib import Path
import pyspark
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

assert sys.version_info[:2] == (3, 11), sys.version
assert pyspark.__version__ == "3.5.8", pyspark.__version__

with tempfile.TemporaryDirectory(prefix="dsa5208-spark-check-") as temp:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("DSA5208-project2-environment-check")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(Path(temp) / "warehouse"))
        .getOrCreate()
    )
    try:
        java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
        assert str(java_version).split(".")[0] == "17", java_version
        assert spark.version == "3.5.8", spark.version
        assert spark.range(5).count() == 5
        data = spark.createDataFrame(
            [(0, 0.0, Vectors.dense(0.0)), (1, 0.0, Vectors.dense(0.2)),
             (2, 1.0, Vectors.dense(0.8)), (3, 1.0, Vectors.dense(1.0))],
            ["id", "label", "features"],
        )
        model = LogisticRegression(maxIter=5, regParam=0.1).fit(data)
        model_path = str(Path(temp) / "model")
        model.write().save(model_path)
        loaded = LogisticRegressionModel.load(model_path)
        before = model.transform(data).orderBy("id").select("probability").collect()
        after = loaded.transform(data).orderBy("id").select("probability").collect()
        assert len(before) == len(after) == 4
        assert all(abs(a.probability[1] - b.probability[1]) < 1e-8
                   for a, b in zip(before, after))
        print(f"OK: Python {sys.version.split()[0]}, Spark {spark.version}, Java {java_version}")
    finally:
        spark.stop()
PY
```

正式实验前还应记录 CPU/RAM、OS、`java -version`、`python --version`、`python -m pip freeze`、Spark 资源配置和数据规模；完整实验环境由 A 填入 `docs/`。C 的本地合成测试记录见 `docs/C/c_foundation_validation.md`。

## 数据与建模约定

- 时间使用 SGT；首先审计原始时间是否对齐五分钟，区分观测时间和更新时间。
- 缺失不等于 0；重复、异常值、站点变动和覆盖率需要有处理记录。
- 标签为站点在 `(t, t+30min]` 六个有效五分钟时隙累计雨量 **严格大于** `threshold_mm`；未来时隙不完整时不能标为无雨。
- 特征只使用预测时已可获得的信息；未来累计量只能作为标签来源。
- 开发时按时间划分训练、验证/校准与测试，避免随机切分相邻窗口导致泄漏；2024 留作最终预测测试。
- 输出并评估概率，至少比较基线，报告逐阈值区分能力和概率质量；不能只报告 Accuracy。
- 站点间的 mm 不能直接相加称为全国总降雨量；跨年比较需控制站点和覆盖率变化。

标签和切分实现位于 `src/task2/labels.py`。正式训练、分析和推理程序仍待实现。

## 数据管道（同学A：P1-01 至 P1-04）

已实现，在人工构造的测试数据上通过 `pytest`，并已于 2026-10-06 在全量数据上运行；结果见 `reports/tables/`，耗时与环境见 `docs/A/environment.md`。

```bash
# 0. 激活环境（见上文“统一环境基线”）后，在仓库根目录执行
python -m pytest -q tests                               # 清洗规则与覆盖率口径测试（约 20 秒）

# 1. 下载 2017–2024 八个年度 CSV（约 9.2 GB），写 data/manifest.csv（字节数、SHA-256、表头、行数）
python src/download_data.py --config configs/project.yaml
#    可选：--years 2017 2024 只下载部分年份；中断后重跑会续传

# 2. 原始审计：准确行数、字段画像、读数类型/单位、时间相位
python src/data_quality.py --config configs/project.yaml --stage raw
#    先看 reports/tables/time_phase_by_year.csv，再确认 configs/project.yaml 的对齐参数

# 3. 清洗、去重、站点元数据；--write-samples 给 B、C 写小样本到 data/samples/
python src/clean_data.py --config configs/project.yaml --write-samples
#    只改了对齐以外的规则时可加 --skip-parse 复用 data/interim/parsed

# 4. 覆盖率、缺口与更新延迟审计
python src/data_quality.py --config configs/project.yaml --stage clean

# 5. 记录实际计算环境
python src/record_environment.py --config configs/project.yaml
```

| 脚本 | 主要产物 |
|---|---|
| `src/download_data.py` | `data/raw/rainfall_<year>.csv`（只读保留）、`data/manifest.csv` |
| `src/data_quality.py --stage raw` | `reports/tables/raw_inventory.csv`、`field_profile.csv`、`reading_type_unit.csv`、`time_phase_by_year.csv`、`time_phase_by_station_year.csv`、`time_seconds_by_year.csv`、`raw_reject_preview.csv` |
| `src/clean_data.py` | `data/clean/clean_rainfall`（Parquet，键 `station_id, timestamp_sgt`）、`quarantine`、`conflicts`、`stations`、`station_phases`；`reports/tables/cleaning_log.csv`、`station_metadata.csv`、`station_phases.csv`、`extreme_candidates.csv` |
| `src/data_quality.py --stage clean` | `data/clean/quality/station_{day,month,year}_coverage`；`reports/tables/station_month_coverage.csv`、`station_year_coverage.csv`、`clean_overview_by_year.csv`、`update_latency_summary.csv`、`update_timestamp_agreement.csv`、`gap_distribution.csv` |

字段含义、清洗 fate 和覆盖率口径见 `docs/A/data_dictionary.md`；时间规则与待冻结参数见 `docs/A/time_semantics.md`；已做决定见 `docs/A/decisions.md`；实际环境填入 `docs/A/environment.md`。

内存：`configs/project.yaml` 默认 `driver_memory: 6g`、`local[*]`。16 GB 内存的机器保持默认；32 GB 可改为 12g。中间结果都落盘为 Parquet，全量 CSV 不会整体装入内存。

## C 基础任务运行与交接

安装上面的统一环境后，在仓库根目录执行：

```bash
source .venv/bin/activate
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
# JAVA_HOME 必须指向本机的 JDK 17。
python -m pip check
python -m pytest -q
```

测试包括人工序列、缺失/无效/重复观测、跨年月、严格阈值、时间切分、
fixture 指标，以及 MLlib Pipeline 保存/加载一致性。临时 smoke-test 模型由
pytest 临时目录管理，不是最终课程模型。

调用顺序：

```python
from src.task2.labels import build_future_30m_base, assign_temporal_split, expand_thresholds

spark.conf.set("spark.sql.session.timeZone", "Asia/Singapore")
# observations 是满足 docs/C/task2_contract.md 的 Spark DataFrame。
base = build_future_30m_base(observations)
split_base = assign_temporal_split(base)
expanded = expand_thresholds(split_base, [0, 0.2, 1, 2, 5, 10])
usable = expanded.where("split IS NOT NULL AND label IS NOT NULL")
```

- A 接口与实际 H1 验收：`docs/C/task2_contract.md`、`reports/c_results/h1_acceptance.json`。
- B 预测表接口：`docs/C/prediction_schema.md`。
- B 可用夹具：`data/samples/predictions_fixture.csv`；仅合成数据，不得用于报告。
- C 验证记录：`docs/C/c_foundation_validation.md`。

## C 完整 Task 2 运行、模型与交付

**A 验收后的接口修订：** 当前推荐 `python -m src.predict_safe`（v2），默认拒绝越界时间和 q>10，明确报告无历史回退，并对支持范围内的概率做单调修正。原 `src.task2.predict` 和 c-models-v1 仅保留用于复现原实验。**原报告的 0.019248 等测试成绩属于 v1，不是 v2 的成绩。** 详见 [五项修订与接口说明](docs/C/inference_v2.md)。

以下命令在仓库根目录、上述 Python/JDK 环境中运行。从头训练需使用**完整 Git 克隆**（`prepare` 会读取历史提交 `21ee7ad` 的来源清单），且该克隆不能已有同名模型/特征/预测目录。Git 中的 `reports/c_results` 是已完成实验的证据，先整体归档到新名称，保留原始测试标记；这是在新克隆中复现实验，不是删除原运行的标记后调参重测。仅部署 ZIP 中的模型预测不需要 Git 历史。

```bash
mv reports/c_results reports/c_results_reference
python src/download_data.py
python -m src.task2.reproduce_clean
python -m src.task2.run prepare
python -m src.task2.run train
python -m src.task2.run calibrate
python -m src.task2.run test
python -m src.task2.verify_artifacts
python -m src.task2.report
python -m src.task2.package_artifacts
```

`reproduce_clean` 不改 A 的清洗逻辑，只使用 C 的 local[4]/4g/32 分区配置及独立审计输出。`prepare` 核对原始文件 SHA-256；主模型只在 2017–2022 拟合。2023 前九个月选模型、后三个月校准；`calibrate` 冻结模型/配置/源码/抽样特征哈希，`test` 检查冻结状态并只允许首次打开 2024。详见 `docs/C/model_protocol.md`。

训练使用预先固定、与标签无关的 0.2% 时刻抽样（69,149 个），验证/校准/测试各约 0.5%（25,227/8,500/31,798 个）。完整数据均参与清洗和标签构建；不声称所有 4,853 万行都用于拟合。自然类别比例保留，不做正负样本重平衡。

加载交付模型预测：

```bash
python -m src.predict_safe S06 '2024-11-07 20:50:00' 1
# 可选部署配置（不是额外的用户预测输入）：
python -m src.predict_safe S06 '2024-11-07 20:50:00' 1 \
  --history data/clean/clean_rainfall --model-root models/final/c_available_v1
```

三项位置参数依次为站点、预测时间、雨量阈值 mm。**无时区时间按 Asia/Singapore（UTC+08:00）解释；必须是整五分钟、秒和微秒为 0，不自动取整。** v2 拒绝 2017–2024 之外或站点实际历史范围之外的时间、未知站点和 q>10 mm。内部缺测仍可能使用日历模型，但会在 JSON 和 stderr 明确警告。返回的 `probability` 是固定六阈值曲线经单调投影与插值后的概率；另保留原始/未修正概率用于诊断。支持 q∈[0,10]，不外推；精确 0/1 会警告，不表示不可能/必然。**不是实时采集服务，也不是可直接部署的暴雨报警系统。** 用 `python -m src.predict_safe --help` 查看规则。

| 交付物 | 路径 |
|---|---|
| 实验方法、结果讨论、团队交接 | `docs/C/model_protocol.md`、`results_discussion.md`、`handoff.md` |
| 分割/标签计数、全部指标与冻结证据 | `reports/c_results/` |
| 可用于小组报告的表格、三张图 | `reports/c_results/result_tables.md`、`reports/c_results/figures/` |
| Spark 模型与校准器（不进入普通 Git） | `models/final/c_available_v1/` |
| 真实验证/校准/测试预测（Parquet） | `data/predictions/c_available_v1/` |
| 当前 v2 安全接口交付包（同一模型权重） | [下载 c_handoff_v2.zip](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v2/c_handoff_v2.zip) |
| 历史 v1 模型交付包（复现原成绩） | [下载 c_handoff_v1.zip](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v1/c_handoff_v1.zip) |

交付 ZIP 包含代码、报告材料、全部候选模型/校准器、预测和抽样特征，不含原始/清洗全量数据、虚拟环境或最终小组 PDF。先解压到仓库外的临时目录；对当前 checkout 仅安装 data/features、data/predictions 和 models/final，不要用旧包源码覆盖当前代码/报告。再按依赖说明准备环境；推理所需 `data/clean/clean_rainfall` 可由下载/清洗命令复现。v1 ZIP 校验值见 `reports/c_results/artifact_manifest.json`；v2 见 `reports/c_results/safe_v2/artifact_manifest.json`。Git 分支/PR 不包含被忽略的大型产物，不能代替这个模型交付包。

### 下载 v2 修订接口

[Release c-models-v2](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/tag/c-models-v2) 包含新接口、测试及 2023 S50 验收记录，并保留原模型权重和 v1 证据。下载 [ZIP](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v2/c_handoff_v2.zip) 和 [校验文件](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v2/c_handoff_v2.zip.sha256)，运行 `shasum -a 256 -c c_handoff_v2.zip.sha256`。版本清单见 `reports/c_results/safe_v2/`。**运行新接口须使用 `python -m src.predict_safe`，原命令不会悄悄改成 v2。** 包内原 test 指标明确仍属于 v1。B 后续补充结果见 `reports/b_model_review/test_serving_v2_posthoc.json`，属于已查看 2024 样本上的事后评估，非新未触碰留出测试。

v2 ZIP：19,435,432 字节；SHA-256 为 `490bc95b6b282b43739fbf7ee8c920cbb1df14e5e572b8ad09f7fcb80aa046cd`。完整回归测试 72 项通过；原 v1 冻结文件哈希保持不变。

### 下载历史 v1 模型包（复现原测试）

- [Release：c-models-v1](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/tag/c-models-v1)（预发布，等待团队验收；不会自动合并 PR）。
- [模型 ZIP](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v1/c_handoff_v1.zip)：19,198,366 字节，395 个条目。
- [SHA-256 校验文件](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v1/c_handoff_v1.zip.sha256)。

SHA-256：

```text
42b149156ee9e9ec99f203cb4a26f911ad4937852a7fb43731c559a1a04dc3a9
```

将 ZIP 和校验文件下载到同一目录后运行：

```bash
shasum -a 256 -c c_handoff_v1.zip.sha256
# Linux 也可使用：sha256sum -c c_handoff_v1.zip.sha256
```

预期输出 `c_handoff_v1.zip: OK`。私有仓库的附件仍要求仓库访问权限；请登录有权限的 GitHub 账号。务必下载上述命名的 ZIP 附件，而不是 GitHub 自动生成的 Source code 压缩包，后者不含被忽略的模型文件。Release 指向交付提交 `c0885c5`，附件保持原始冻结快照；此后新增的下载说明只更新 Git 文档，不改 ZIP 或模型。

如果是**复核已交付的冻结模型**（而非从头训练新实验），请用 `python src/download_data.py --config configs/c_download_preserve_manifest.yaml` 下载，再运行 `python -m src.task2.reproduce_clean` 和 `python -m src.task2.verify_artifacts`。此下载专用配置将新下载时间写入 `logs/c_reload_download_manifest.csv`，保留交付包中已冻结的 `data/manifest.csv`；否则默认下载会改动清单时间，哈希检查将正确拒绝。这条复核路径不重复打开测试评估，也不重新训练。

## Git 与协作

- 默认主分支为 `main`；各成员在自己的功能分支开发，通过 review 后合并。
- 每次增加依赖先更新 `requirements.txt`，三人共同验证；正式实验记录完整安装版本。
- `.gitignore` 排除全量数据、模型文件、日志、凭据和运行缓存；`.gitkeep` 是空文件，不含配置或业务内容。
- 保留下载来源与数据版本，后续模型另行归档并随课程要求提交；被 Git 忽略不等于可以省略模型交付。
- 小组仍需人工核对最终报告/实际 AI 声明、接口适用范围，并提交 Canvas；本轮独立技术复核、全文报告及姓名学号已完成。


## B 分析与独立评估

B 的开发指南见 [docs/B_DEVELOPMENT.md](docs/B_DEVELOPMENT.md)，交接状态见
[docs/B_HANDOFF.md](docs/B_HANDOFF.md)。分析、独立复算和图表代码位于 `src/b_analysis/`。
运行 `source scripts/b_env.sh` 后可执行 `python -m pytest -q`；人工演示用
`bash scripts/run_b_demo.sh reports/demo_b_new`，人工结果不得作为正式项目结论。

B 的最新真实数据分析与交接状态：[docs/B/STATUS.md](docs/B/STATUS.md)。
真实工作稿：[reports/b_results/B_RESULTS.md](reports/b_results/B_RESULTS.md)。
B 审查发现的 C 严格阈值浮点边界问题已由 C 修复，原 strict xfail 测试现为正常回归测试。B 的原审查记录保留历史状态；实际模型预测的独立复算仍待 B 完成，不能用合成夹具验收代替。


## B 完整复核与 PDF 复现（2026-10-12）

先按上文准备 A 的清洗数据、校验并安装 C 模型包里的模型、预测和抽样特征。以下命令只复核已保存预测，不重训、不改变 v1 冻结文件。完整表是导出的自然分布抽样预测全集，不是八年每个时隙都执行模型。

```bash
source scripts/b_env.sh
python -m scripts.review_b_models
python -m scripts.plot_b_model_review
python -m pip install -r requirements-report.txt
python -m scripts.build_group_report
python -m pytest tests -q
```

报告渲染可单独使用含 ReportLab/Pillow 的 Python 环境，不需要启动 Spark。生成 `output/pdf/DSA5208_Project2_Report.pdf`、同内容的 `reports/DSA5208_Project2_Report.pdf` 和 `reports/GROUP_REPORT.md`。B 原 Task 1 全量复现需新输出目录，例如 `bash scripts/run_b_real.sh reports/b_reproduction`；该目录若已存在，应另选空目录。不重新运行训练/选模来覆盖冻结实验。

私有 Release 匿名请求可能返回 404，需登录有权限的账户；已在 B 本机通过认证 API 下载 v2 ZIP 并验证 SHA-256 和 CRC，未将凭据写入项目。


提交候选包（不执行 Canvas 上传）：

```bash
python -m scripts.package_group_submission
```

输出 `output/submission/DSA5208_Project2_submission.zip` 及 `.sha256`。内含当前代码/README、全文 PDF、实际 Pipeline 模型/校准器、抽样特征/预测和逐文件 SHA-256 清单；不含原始 CSV、全量清洗数据、凭据或运行环境。打包前核对冻结模型哈希，打包后核对每个成员文件及 ZIP CRC。Canvas 若要求分别上传，按包内目录拆出源码、模型和 PDF。
