import sys
import pyspark
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler


def test_spark_mllib_save_reload(spark, tmp_path):
    assert sys.version_info[:2] == (3, 11)
    assert pyspark.__version__ == spark.version == "3.5.8"
    assert spark.sparkContext._jvm.java.lang.System.getProperty("java.version").split(".")[0] == "17"
    assert spark.range(5).count() == 5
    data = spark.createDataFrame([(0, 0., 0.), (1, .2, 0.), (2, .8, 1.), (3, 1., 1.)], "id int, x double, label double")
    model = Pipeline(stages=[VectorAssembler(inputCols=["x"], outputCol="features"),
                             LogisticRegression(maxIter=5, regParam=.1)]).fit(data)
    path = str(tmp_path / "pipeline")
    model.write().save(path)
    loaded = PipelineModel.load(path)
    before = model.transform(data).orderBy("id").select("probability").collect()
    after = loaded.transform(data).orderBy("id").select("probability").collect()
    assert all(abs(a.probability[1] - b.probability[1]) < 1e-8 for a, b in zip(before, after))
