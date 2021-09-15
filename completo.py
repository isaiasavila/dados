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


df_join = df_ex.join(df_csv, df_ex.Ranking == df_csv.rank, 'left') 
# df_join.show(20)
# print(type(df_join))#<class 'pyspark.sql.dataframe.DataFrame'>
# z = df_join.select('age').count()
# print(z) #7917


df_join = df_join.toDF(*['Idade', 'Categoria', 'Nome', 'Nac', 'rank', 'Gênero', 'Esporte', 'Seleção', 'Ranking', 'rank1'
                        , 'Medalha', 'count'])

df_join = df_join.drop('rank', 'rank1', 'count')
# df_join.show()

path = "C:/Users/Ricardo Felipe/Desktop/SoulCode/Engenharia de Dados/Projeto FInal/population_csv.csv"

df_w2 = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
df_w2 = df_w2.select(['Country Name','Year','Value']).filter(df_w2['Year'] == '2018')

df_join_w2 = df_join.join(df_w2, df_join.Seleção == df_w2['Country Name'], 'left')
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

# ct = df_join_w2.select('Country Name').count()
# print(ct) #7917

# ===================================================== TRANSFORM ======================================================
pandasDF = df_join.toPandas()
# print(type(pandasDF)) # <class 'pandas.core.frame.DataFrame'>

df1 = pandasDF[pandasDF['Medalha'] != 'NA']
df1 = df1['Medalha'].value_counts()
# print(df1)


df_join.registerTempTable('temp')
df_sql = spark.sql('SELECT Medalha, Idade, Esporte FROM temp WHERE Medalha != "NA" ORDER BY Idade')
# df_sql = spark.sql(')
df_sql.show(1)
df_sql = spark.sql('SELECT Medalha, Idade, Esporte FROM temp WHERE Medalha != "NA" ORDER BY Idade DESC')
df_sql.show(1)

# df_join1 = df_join.groupby('Idade', 'Medalha').count().sort('Idade').when('Medalha' != 'NA')
# df_join1.show()


# path = "C:/Users/Ricardo Felipe/Desktop/SoulCode/Engenharia de Dados/Projeto FInal/2021_population.csv"
# df_w = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_w = df_w.select(['country','world_%','2021_last_updated'])
# df_w.show()
# ca = df_w.select('country').count()
# print(ca) #228

# df_join_w = df_join.join(df_w, df_join.Seleção == df_w.country, 'left') 
# df_join_w.show()
# cz = df_join_w.select('country').count()
# print(cz) #5678
# zc = df_join_w.select('Idade').count()
# print(zc) #5678
