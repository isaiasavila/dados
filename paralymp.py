from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode_outer

from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('firstSeesion')\
    .config("spark.master","local[4]") \
    .config("spark.executor.memory","1gb") \
    .config("spark.shuffle.sql.partitions",2 ) \
    .getOrCreate()

path = 'C:/Users/Ricardo Felipe/Desktop/SoulCode/Engenharia de Dados/Projeto FInal/paralympics_tokyo.json'
df_json = spark.read.option("multiline","true").json(path)
# df_json.groupby('age').count().show()
# x = df_json.select('age').count()
# print(x) #4525

path = 'C:/Users/Ricardo Felipe/Desktop/SoulCode/Engenharia de Dados/Projeto FInal/Paralympics_tokyo_21.csv'
df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_csv.groupby('medal').count().show()
# y = df_csv.select('age').count()
# print(y) #7917
df_csv = df_csv.groupby('rank', 'medal').count()


df_ex = df_json.select('*', explode_outer(df_json.rank).alias('Ranking'))
# df_ex.show()
# df_ex.printSchema()
# print(type(df_ex)) #<class 'pyspark.sql.dataframe.DataFrame'>


df_join = df_ex.join(df_csv, df_ex.Ranking == df_csv.rank, 'inner') 
# df_join.show(20)
# print(type(df_join))#<class 'pyspark.sql.dataframe.DataFrame'>
# z = df_join.select('age').count()
# print(z) #7919


df_join = df_join.toDF(*['Idade', 'Categoria', 'Nome', 'Nac', 'rank', 'Gênero', 'Esporte', 'Seleção', 'Ranking', 'rank1'
                        , 'Medalha', 'count'])

df_join = df_join.drop('rank', 'rank1', 'count')
df_join.show()
