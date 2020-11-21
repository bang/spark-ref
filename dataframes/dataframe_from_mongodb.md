
## Importing 
```python
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
```
## Connection directly on SparkSession
```python
spark = SparkSession \
    .builder \
    .appName("tgt-santander-ingestion"
    ).config("spark.jars","jars/mongo-spark-connector_2.11-2.4.1.jar,jars/mongo-java-driver-3.11.0-rc0.jar,scala-library-2.11.12.jar"
    ).config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.fake"
    ).config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.fake"
    ).config(
        "spark.hadoop.hive.metastore.warehouse.dir"
        ,"/home/hduser/Projects/job-test/metastore_db"
    ).config("spark.sql.warehouse.dir","/user/hive/warehouse"
    ).enableHiveSupport(
    ).getOrCreate()

mongoDF = spark.read.format("mongo").load()
```

