from datetime import date
from g3utilidades import UtilidadesG3
from pyspark.sql.session import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('Teste') \
    .config("spark.master","local") \
    .config("spark.executor.memory","1gb") \
    .config("spark.shuffle.sql.partitions", 1) \
    .getOrCreate()

schema = StructType([StructField("nome", StringType()),
         StructField("short_name", StringType()),
         StructField("gender", StringType()),
         StructField("birth_date", StringType()),
         StructField("birth_place", StringType()),
         StructField("birth_country", StringType()),
         StructField("country", StringType()),
         StructField("discipline", StringType()),
         StructField("discipline_code", StringType()),
         StructField("residence_place", StringType()),
         StructField("residence_country", StringType()),
         StructField("height_m/ft", FloatType()),
         StructField("url", StringType()),
])         

path = "C:/scripts/athletes.csv"

df = spark.read.format("csv") \
    .schema(schema) \
    .load(path, sep=",", header=True)

# Alterando o nome da coluna
df = df.withColumnRenamed("discipline", "disciplina") 
#df.printSchema()

# Iniciando segunda base de dados
df2 = spark.read.load("C:/scripts/medals.csv", format="csv", sep=",", inferSchema="true", header="true")
#df2.limit(10).show()

# Juntar dois data sets
df3 = df.join(df2, ["country"], how='left')

# Excluindo colunas
df3 =  df3.drop("disciplina", "nome", "medal_code", "medal_date", "athlete_short_name",\
                "athlete_sex", "athlete_link", "country_code", "discipline_code", "short_name",\
                "birth_place", "birth_country", "discipline_code", "residence_place", "residence_country",\
                "height_m/ft", "url")

# 
df3 = df3.withColumn('age',2021 - df3.birth_date.substr(1, 4))\
         .drop('birth_date')

# Objeto criado para utilização do método converterColuna
c = UtilidadesG3()
colunas_inteiro = ['age']
df3 = c.converterColuna(df3, colunas_inteiro, IntegerType())
df3.show(1000)