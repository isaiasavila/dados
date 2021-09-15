from datetime import date
from g3spark import SparkG3
from g3utilidades import UtilidadesG3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.functions import explode_outer

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


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Esquema da base de dados
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
# Caminho do arquivo que será utilizado
path = "C:\scripts\dados/athletes.csv"
# Parâmetros de conexão
df = spark.read.format("csv") \
    .schema(schema) \
    .load(path, sep=",", header=True)
# Alterando o nome de uma das colunas, ação necessária para o drop futuro
df = df.withColumnRenamed("discipline", "disciplina")
# Caminho do próximo arquivo que será utilizado
path = "C:\scripts\dados/medals.csv"
# Iniciando segunda base de dados
df2 = spark.read.load(path, format="csv", sep=",", inferSchema="true", header="true")
# Juntar dois data sets
df3 = df.join(df2, ["country"], how='left')
# Excluindo colunas que não serão utilizadas
df3 =  df3.drop("disciplina", "nome", "medal_code", "medal_date", "athlete_short_name",\
                "athlete_sex", "athlete_link", "country_code", "discipline_code", "short_name",\
                "birth_place", "birth_country", "discipline_code", "residence_place", "residence_country",\
                "height_m/ft", "url")
# Alterando a coluna e excluindo a outra
df3 = df3.withColumn('age',2021 - df3.birth_date.substr(1, 4))\
         .drop('birth_date')
# Objeto criado para utilização do método converterColuna
util = UtilidadesG3()
# Lista das colunas que serão alteradas
colunas_inteiro = ['age']
# Chamada do método que fará a alteração na coluna
df3 = util.converterColuna(df3, colunas_inteiro, IntegerType())
#df3.show()

# Caminho do próximo arquivo que será utilizado
path = "C:\scripts\dados/population_csv.csv"

df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')

dfp = pd.read_csv('C:\scripts\dados/population_csv.csv')
# print(dfp)

df_csv = df_csv.select(['Country Name','Year','Value']).filter(df_csv['Year'] == '2018')
#df_csv.show(50)
#df_csv.printSchema()
colunas_inteiro = ['Value']

df_csv = util.converterColuna(df_csv, colunas_inteiro, IntegerType())
#df_csv.printSchema()

df_join = df3.join(df_csv, df_csv['Country Name'] == df3['country'], how='left')

# df_join.filter(df_join['value'] == 'null').groupBy()
# print('...')
df_join.groupby('country', 'Value').count().sort('Value')
# x = df3.select(['country']).count()
# y = df_csv.select(['Country Name']).count()
# z = df_join.select(['Country Name']).count()
# print ('contagem de linhas \n ',x,'...', y,'...', z)
# input('.....')
df_join = df_join.select(['athlete_name','age','gender','discipline','medal_type','country','value'])

df_join = df_join.dropna()

# Excusão d
df_join = df_join.drop_duplicates()

# Renomeação dos cabeçalhos para o português

df_join = df_join.toDF(*['Atleta', 'Idade', 'Gênero', 'Modalidade', 'Medalha', 'País', 'População_Total'])
# df_join.show(100)

# df_join = df_csv.join(df3, df_csv['Country Name'] == df3['country'], how='left')
# df3.join(df3, df_csv['Country Name'] == df3['country'], how='left').show()
# df_join.show(100)