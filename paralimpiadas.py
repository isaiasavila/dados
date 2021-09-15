from datetime import date
from g3spark import SparkG3
from g3utilidades import UtilidadesG3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.functions import explode_outer
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

# Objeto spark criado e o método para criar uma sessão é chamado
spark = SparkG3().iniciar_sessao()

path = 'C:\scripts\dados/paralympics_tokyo.json'
df_json = spark.read.option("multiline","true").json(path)
# df_json.groupby('age').count().show()
# x = df_json.select('age').count()
# print(x) #4525

path = 'C:\scripts\dados/Paralympics_tokyo_21.csv'
df_csv1 = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_csv1.groupby('medal').count().show()
# y = df_csv1.select('age').count()
# print(y) #7917
df_csv1 = df_csv1.groupby('rank', 'medal').count()

df_ex = df_json.select('*', explode_outer(df_json.rank).alias('Ranking'))
# df_ex.show()
# df_ex.printSchema()
# print(type(df_ex)) #<class 'pyspark.sql.dataframe.DataFrame'>

df_join1 = df_ex.join(df_csv1, df_ex.Ranking == df_csv1.rank, 'left') 
# df_join1.show(20)
# print(type(df_join1))#<class 'pyspark.sql.dataframe.DataFrame'>
# z = df_join1.select('age').count()
# print(z) #7917

df_join1 = df_join1.toDF(*['Idade', 'Categoria', 'Nome', 'Nac', 'rank', 'Gênero', 'Esporte', 'Seleção', 'Ranking', 'rank1'
                        , 'Medalha', 'count'])

df_join1 = df_join1.drop('rank', 'rank1', 'count')
# df_join1.show()

path = "C:\scripts\dados/population_csv.csv"

df_w2 = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
df_w2 = df_w2.select(['Country Name','Year','Value']).filter(df_w2['Year'] == '2018')

df_join_w2 = df_join1.join(df_w2, df_join1.Seleção == df_w2['Country Name'], 'left')
# df_join_w2.show()

df_join_w2 = df_join_w2.toDF(*['Idade', 'Evento', 'Atleta', 'Nac', 'Gênero', 'Modalidade', 'País', 'Ranking', 'Medalha'
                        , 'country', 'year', 'População_Total'])

df_join_w2 = df_join_w2.select('Atleta', 'Idade', 'Gênero', 'Modalidade', 'Medalha', 'País', 'População_Total')
# df_join_w2.show()
# df_join_w2.printSchema()

def colunaTipo(dataframe, nomes, novoTipo):
    for nome in nomes:
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe

colunasI = ['Idade', 'População_Total']

df_join_w2 = colunaTipo(df_join_w2, colunasI, IntegerType())

# df_join_w2.printSchema()
# df_join_w2.show()
# ct = df_join_w2.select('Country Name').count()
# print(ct) #7917

# ===================================================== TRANSFORM ======================================================
pandasDF = df_join1.toPandas()
# print(type(pandasDF)) # <class 'pandas.core.frame.DataFrame'>

df1 = pandasDF[pandasDF['Medalha'] != 'NA']
df1 = df1['Medalha'].value_counts()
# print(df1)

df_join1.registerTempTable('temp')
df_sql = spark.sql('SELECT Medalha, Idade, Esporte FROM temp WHERE Medalha != "NA" ORDER BY Idade')
# df_sql = spark.sql(')
#df_sql.show(1)
df_sql = spark.sql('SELECT Medalha, Idade, Esporte FROM temp WHERE Medalha != "NA" ORDER BY Idade DESC')
#df_sql.show(1)

# df_join2 = df_join1.groupby('Idade', 'Medalha').count().sort('Idade').when('Medalha' != 'NA')
# df_join2.show()

paraolimpiadas = df_join_w2.toPandas()

def get_database():
    from pymongo import MongoClient
    import pymongo
    CONNECTION_STRING = "mongodb+srv://isaias:isaias@cluster0.h4ljz.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    from pymongo import MongoClient
    client = MongoClient (CONNECTION_STRING)

    return client ['projetofinal']

dbname = get_database()
collection_name = dbname['paraolimpiadas2']

data_dict = paraolimpiadas.to_dict('records')
collection_name.insert_many(data_dict)
print("DF importado")