from datetime import date
from g3spark import SparkG3
from g3utilidades import UtilidadesG3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

# Objeto spark criado e o método para criar uma sessão é chamado
spark = SparkG3().iniciar_sessao()
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
path = "C:/scripts/athletes.csv"
# Parâmetros de conexão
# df1 e df2 serão usados como dataFrames temporários
df_atletas = spark.read.format("csv") \
    .schema(schema) \
    .load(path, sep=",", header=True)
# Alterando o nome de uma das colunas, ação necessária para o drop futuro
df_atletas = df_atletas.withColumnRenamed("discipline", "disciplina")
# Caminho do próximo arquivo que será utilizado
path = "C:/scripts/medals.csv"
# Iniciando segunda base de dados
df_medalhas = spark.read.load(path, format="csv", sep=",", inferSchema="true", header="true")

df_medalhas = df_medalhas.withColumnRenamed("country", "namecountry")
# Juntar dois data sets |||||||||||||||| Código da Nicole' bad ||||||||||||||||
#df_atletas_medalhas = df_atletas_medalhas.join(df_medalhas, ["country"], how='left')
df_atletas_medalhas = df_atletas.join(df_medalhas,\
     df_medalhas['namecountry'] == df_atletas['country'], how='left')

# Excluindo colunas que não serão utilizadas
df_atletas_medalhas =  df_atletas_medalhas.drop("disciplina", "nome", "medal_code", "medal_date", "athlete_short_name",\
                "athlete_sex", "athlete_link", "country_code", "discipline_code", "short_name",\
                "birth_place", "birth_country", "discipline_code", "residence_place", "residence_country",\
                "height_m/ft", "url")
# Alterando a coluna e excluindo a outra
df_atletas_medalhas = df_atletas_medalhas.withColumn('age',2021 - df_atletas_medalhas.birth_date.substr(1, 4))\
         .drop('birth_date')
# Apagar se der certo
df_atletas_medalhas = df_atletas_medalhas.dropna(how='any')

# df_atletas_medalhas.select(['country','medal_type','gender']).filter\
#                           (df_atletas_medalhas['country'] == 'Chile').show(20)
# Objeto criado para utilização do método converterColuna
util = UtilidadesG3()
# Lista das colunas que serão alteradas
colunas_inteiro = ['age']
# Chamada do método que fará a alteração na coluna
df_atletas_medalhas = util.converterColuna(df_atletas_medalhas, colunas_inteiro, IntegerType())
# Caminho do próximo arquivo que será utilizado
path = "C:/scripts/population_csv.csv"
# path = 'https://raw.githubusercontent.com/isaiasavila/dados/main/population_csv.csv'


df_populacao = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')


# Pandas
# dfp = pd.read_csv('https://raw.githubusercontent.com/isaiasavila/dados/main/population_csv.csv')
# print(dfp)

df_populacao = df_populacao.select(['Country Name','Year','Value']).filter(df_populacao['Year'] == '2018')
colunas_inteiro = ['Value']
df_populacao = util.converterColuna(df_populacao, colunas_inteiro, IntegerType())
# Método para troca
# # Troca o nome dos países para o padrão do dataSet do projeto
# df1 = df_populacao.withColumn('Country Name', F.when(F.col('Country Name') == 'United States', 'United States of America')\
#             .otherwise(F.col('Country Name')))



#df1.filter(df1['Country Name'] == 'United States of America').select('Country Name','Value').show(50)

df_atletas_medalhas_pop = df_atletas_medalhas.join(df_populacao,\
     df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left')
df_atletas_medalhas_pop.show(500)

print('...\n\n\n...')

df_pop_atletas_medalhas = df_populacao.join(df_atletas_medalhas, \
    df_populacao['Country Name'] == df_atletas_medalhas['country'], how='right')
#df_pop_atletas_medalhas.show(5)

x = df_atletas_medalhas_pop.select(['country']).count()
y = df_pop_atletas_medalhas.select(['country']).count()
print ('contagem de linhas \n ',x,'...', y,'...')
df1 = df_pop_atletas_medalhas.groupBy('country','Country name','Value').count().sort('Value').limit(36).toPandas()
#print(df1)
# Mostrando os países (distintos) agrupados por população filter('Value' == 'null')
print('...\n\n\n...')
# Visualizando 
#df1 = df_atletas_medalhas_pop.groupby('country', 'Value').count().filter(df_atletas_medalhas_pop['country'] != 'Argentina')
#df1 = df_atletas_medalhas_pop.groupby('country', 'Value').count().filter(df_atletas_medalhas_pop['Value'] )
# Buscando a primeira lista de países
df2 = df_atletas_medalhas_pop.groupBy('country','Country name','Value').count().sort('Value').limit(35).toPandas()
#print(df2)
# df1 = df1.sort_values()
# df2.replace({'country':{'"': ''}})
lista2 = []
lista1 = df1['country']



#print(lista1)

# lista1  = df1['country']
# lista2 = df_populacao['Country Name']

# df_atletas_medalhas_pop.filter(df_atletas_medalhas_pop['value'] == 'null').groupBy()
print('..........->->->')
#
# x = df_atletas_medalhas.select(['country']).count()
# y = df_populacao.select(['Country Name']).count()
# z = df_atletas_medalhas_pop.select(['Country Name']).count()
# print ('contagem de linhas \n ',x,'...', y,'...', z)

df_atletas_medalhas_pop = df_atletas_medalhas_pop.select(['athlete_name','age','gender','discipline','medal_type','country','value'])

df_atletas_medalhas_pop = df_atletas_medalhas_pop.dropna()
# Excusão d
df_atletas_medalhas_pop = df_atletas_medalhas_pop.drop_duplicates()
# Renomeação dos cabeçalhos para o português
df_atletas_medalhas_pop = df_atletas_medalhas_pop.toDF(*['Atleta', 'Idade', 'Gênero', 'Modalidade', 'Medalha', 'País', 'População_Total'])
#df_atletas_medalhas_pop.show(100)
# df_atletas_medalhas_pop = df_populacao.join(df_atletas_medalhas, df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left')
# df_atletas_medalhas_pop.show(20)
#df_atletas_medalhas.join(df_atletas_medalhas, df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left').show()
# df_atletas_medalhas_pop.show(200)