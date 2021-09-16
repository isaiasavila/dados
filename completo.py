from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('firstSeesion')\
    .config("spark.master","local[4]") \
    .config("spark.executor.memory","1gb") \
    .config("spark.shuffle.sql.partitions",2 ) \
    .getOrCreate()

path = 'C:/script/paralympics_tokyo.json'
df_para_json = spark.read.option("multiline","true").json(path)
# df_para_json.groupby('age').count().show()
# x = df_para_json.select('age').count()
# print(x) #4525

path = 'C:/script/Paralympics_tokyo_21.csv'
df_para_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_para_csv.groupby('medal').count().show()
# y = df_para_csv.select('age').count()
# print(y) #7917
df_para_csv = df_para_csv.groupby('rank', 'medal').count()

df_explode = df_para_json.select('*', explode_outer(df_para_json.rank).alias('Ranking'))
# df_ex.show()
# df_ex.printSchema()
# print(type(df_ex)) #<class 'pyspark.sql.dataframe.DataFrame'>

df_para = df_explode.join(df_para_csv, df_explode.Ranking == df_para_csv.rank, 'left') 
# df_para.show(20)
# print(type(df_para))#<class 'pyspark.sql.dataframe.DataFrame'>
# z = df_para.select('age').count()
# print(z) #7917

df_para = df_para.toDF(*['Idade', 'Categoria', 'Nome', 'Nac', 'rank', 'Gênero', 'Esporte', 'Seleção', 'Ranking', 'rank1'
                        , 'Medalha', 'count'])

df_para = df_para.drop('rank', 'rank1', 'count')
# df_para.show()

path = "C:/script/population_csv.csv"

df_pop = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
df_pop = df_pop.select(['Country Name','Year','Value']).filter(df_pop['Year'] == '2018')

df_para_pop = df_para.join(df_pop, df_para.Seleção == df_pop['Country Name'], 'left')
# df_para_pop.show()

df_para_pop = df_para_pop.toDF(*['Idade', 'Evento', 'Atleta', 'Nac', 'Gênero', 'Modalidade', 'País', 'Ranking', 'Medalha'
                        , 'country', 'year', 'População_Total'])

df_para_pop = df_para_pop.select('Atleta', 'Idade', 'Gênero', 'Modalidade', 'Medalha', 'País', 'População_Total')
# df_para_pop.show()
# df_para_pop.printSchema()

def colunaTipo(dataframe, nomes, novoTipo):
    for nome in nomes:
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe

colunasI = ['Idade', 'População_Total']

df_para_pop = colunaTipo(df_para_pop, colunasI, IntegerType())

# df_para_pop.printSchema()

# ct = df_para_pop.select('País').count()
# print(ct) #7917

# ===================================================== TRANSFORM ======================================================
# pandasDF = df_join.toPandas()
# print(type(pandasDF)) # <class 'pandas.core.frame.DataFrame'>

# df1 = pandasDF[pandasDF['Medalha'] != 'NA']
# df1 = df1['Medalha'].value_counts()
# print(df1)

#==========================================================SQL==========================================================
# df_join.registerTempTable('temp')
# df_sql = spark.sql('SELECT Medalha, Idade, Esporte FROM temp WHERE Medalha != "NA" ORDER BY Idade')
# df_sql = spark.sql(')
# df_sql.show(1)
# df_sql = spark.sql('SELECT Medalha, Idade, Esporte FROM temp WHERE Medalha != "NA" ORDER BY Idade DESC')
# df_sql.show(1)

#==========================================================SQL========================================================
