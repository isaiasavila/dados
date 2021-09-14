from pyspark.sql.session import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('firstSeesion')\
    .config("spark.master","local[4]") \
    .config("spark.executor.memory","1gb") \
    .config("spark.shuffle.sql.partitions",2 ) \
    .getOrCreate()


df = spark.read.json("C:\scripts\Trabalhofinal\EMA_definition.json")
# # Define custom schema
# schema = StructType([
#       StructField("name",StringType()),
#       StructField("questions",FloatType()),
#       StructField("options",StringType()),
#       StructField("question_id",StringType()),
#       StructField("question_text",StringType()),
# ])

# df_with_schema = spark.read.schema(schema) \
#     .json("C:\scripts\Trabalhofinal\EMA_definition.json")
# df_with_schema.printSchema()
# df_with_schema.show() 

df = spark.read.option("multiline","true").json("C:\scripts\Trabalhofinal\EMA_definition.json")
df.show()