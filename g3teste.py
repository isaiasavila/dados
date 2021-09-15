from g3spark import SparkG3
from g3mongo import MongoG3
import pandas as pd
import pyspark.sql.functions as F
import cerberus
from pyspark.sql.functions import *

# Conexão no MongoDB
# conexao = MongoG3()
# detalhes_items = conexao.conecta_mongo("teste")
# df = pd.DataFrame(list(detalhes_items))
# print(df)
spark = SparkG3().iniciar_sessao()
path = "C:/scripts/paralympics_tokyo.json"
rawDF = spark.read.json(path, multiLine = "true")
#rawDF.printSchema()

# índice campo
#batDF = sampleDF.select("ev") # mesmo nome ali de cima
#batDF.printSchema()
# mostrar
#batDF.show(300)
#batDF.show(1, False)
df_json = rawDF.withColumnRenamed("event", "ev")
df_json = df_json.select("*", explode("ev").alias("event"))
df_json = df_json.drop('ev')
df_json = rawDF.withColumnRenamed("rank", "rk")
df_json = df_json.select("*", explode("rk").alias("rank"))
df_json = df_json.drop('rk')
print(type(df_json))
#sampleDF.show(200)

# explode_outer = []
# df_json = conexao.iniciar_sessao().read.option("multiline", "true").json(path)

# df_json.select(['name','noc'],(explode_outer(df_json.rank))).show(10)
# input('...|||...')

# df.withColumn("data", from_json("data", schema))\
#     .select(col('id'), col('point'), col('data.*'))\
#     .show()

# df_json \
# .select(collect_list(struct(F.col("*"))).alias("data")) \
# .withColumn("list",F.array([F.lit(i) for i in list_data])) \
# .select(F.explode(F.arrays_zip(F.col("data"),F.col("list"))).alias("full_data")) \ 
# .select(F.col("full_data.data.*"),F.col("full_data.list").alias("col3")) \ 
# .show()

# Separar linhas por delimitador.
#rdd = rdd.map(lambda line: line.split(","))
# Retornas as primeiras linhas.
#rdd.take(2)


# df_json.show(30)
# df_json.printSchema()
# df_json.select('sex').count() #não retorna nada, converti para Pandas:
#converter para Dataframe do pandas



# ------------------------------ Não esquecer da validação
# schema = {'age': {'type': 'string'}}
# v = cerberus.Validator(schema)
# document = {'name': 'john doe'}
# v.validate(document)
# pandasDf = df_json.toPandas()
path = 'C:/scripts/Paralympics_tokyo_21.csv'
df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')

df_join = df_json.join(df_csv)
print('...')
df_join = df_json.join(df_csv,df_json['age'] ==  df_csv['age'], how='left')

print('...')
df_join.first()
df_join.show(100, truncate=True)

input('ok')
# pandasDf = df_csv.toPandas()

# print(f'A tabela possui {pandasDf.shape[0]} linhas e {pandasDf.shape[1]} colunas!\n')


#df = df.withColumn("data_type", F.lit(df.schema.["Frequency"].dataType))
#df = df.withcolumn('typ_freq',F.when(F.col("data_type") != dic["Frequency"], False).otherwise('true'))
# df_csv.show()
# df_csv.printSchema()
#df_csv.select('rank').count()

#=================================NÚMERO DE LINHAS DIFERENTES ENTRE O CSV E O JSON, JOIN INPOSSÍVEL==========================

#print(pandasDf['rank'].value_counts()) #927 valores únicos, muitos arrays

# df_json = df.drop('name')
# df_csv = df_csv.select('medal', 'name', 'rank')

# df_json.show()
# df_csv.show()

# df_join = df_json.join(df_csv,df_json['rank'] ==  df_csv['rank'])
# df_join.show(20)

# df_join = df_join.toDF(*['Age', 'Nacionalidade', 'Sexo', 'Esporte', 'Seleção', 'Medalha', 'Nome'])
# df_join.printSchema()

# def colunaTipo(dataframe, nomes, novoTipo):
#     for nome in nomes:
#         dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
#     return dataframe

# colunaI = ['Age']

# df_join = colunaTipo(df_join, colunaI, IntegerType())

# df_join.show(10)
# df_join.printSchema()

# df_join = df_join.groupby('E')