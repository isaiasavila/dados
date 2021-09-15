from datetime import date
from g3spark import SparkG3
from g3utilidades import UtilidadesG3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

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
df = spark.read.format("csv") \
    .schema(schema) \
    .load(path, sep=",", header=True)
# Alterando o nome de uma das colunas, ação necessária para o drop futuro
df = df.withColumnRenamed("discipline", "disciplina")
# Caminho do próximo arquivo que será utilizado
path = "C:/scripts/medals.csv"
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
# Caminho do próximo arquivo que será utilizado
path = "C:/scripts/population_csv.csv"
df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
df_csv = df_csv.select(['Country Name','Year','Value']).filter(df_csv['Year'] == '2018')


df_join = df3.join(df_csv, df_csv['Country Name'] == df3['country'], how='left')


x = df3.select(['country']).count()
y = df_csv.select(['Country Name']).count()
z = df_join.select(['Country Name']).count()
print ('contagem de linhas \n ',x,'...', y,'...', z)

df_join = df_join.select(['athlete_name','age','gender','discipline','medal_type','country','value'])
df_join.filter(df_join['country'] == 'United States of America').show(100)
df_join = df_join.dropna()
# Excusão d
df_join = df_join.drop_duplicates()
# Renomeação dos cabeçalhos para o português
df_join = df_join.toDF(*['Atleta', 'Idade', 'Gênero', 'Modalidade', 'Medalha', 'País', 'População_Total'])
#df_join.show(100)
# df_join = df_csv.join(df3, df_csv['Country Name'] == df3['country'], how='left')
# df_join.show(20)
#df3.join(df3, df_csv['Country Name'] == df3['country'], how='left').show()
# df_join.show(200)