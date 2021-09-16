from datetime import date
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

class MongoG3():
    def conecta_mongo_colecao(self, tabela):
        '''
        Método para acessar ao repositório especificado do MongoDB de Isaias Avila dos Santos,
        utilizando o usuário e a senha informados como parâmetro, retorna um banco
        O método retorna uma coleção especificada como parâmetro <tabela> no repositório
        repassa o nome do banco, usuario e senha para conexão com o MongoDB
        utilizando o método <get_database(...)> da classe
        '''
        from pymongo import MongoClient
        import pymongo
        # Forneça o url do atlas mongodb para conectar python a mongodb usando pymongo
        try:
            # Lê um arquivo com as informações, para conexão no banco de dados Mongo
            with open('conexao') as arquivoTemporario:
                nl = arquivoTemporario.readlines()
            # Depois de ler o arquivo joga a informação para uma string de conexão
            CONNECTION_STRING = nl[0]
            # Crie uma conexão usando MongoClient. Você pode importar MongoClient ou usar pymongo.MongoClient
            _client = MongoClient(CONNECTION_STRING)
            # Cria o banco de dados.
            _dbname = _client['projetofinal']
            # o parâmetro do método, específica a coleção que será utilizada
            _collection_name = _dbname[tabela]
            # o método retorna uma coleção inteira, muita atenção quando ela for muito grande
            return _collection_name.find()
        except:
            print('Falha na conexão! Tente novamente')

def conecta_mongo(self, tabela):
        '''
        Método para acessar ao repositório especificado do MongoDB de Isaias Avila dos Santos,
        utilizando o usuário e a senha informados como parâmetro, retorna um banco
        O método retorna uma coleção especificada como parâmetro <tabela> no repositório
        repassa o nome do banco, usuario e senha para conexão com o MongoDB
        utilizando o método <get_database(...)> da classe
        '''
        from pymongo import MongoClient
        import pymongo
        # Forneça o url do atlas mongodb para conectar python a mongodb usando pymongo
        try:
            # Lê um arquivo com as informações, para conexão no banco de dados Mongo
            with open('conexao') as arquivoTemporario:
                nl = arquivoTemporario.readlines()
            # Depois de ler o arquivo joga a informação para uma string de conexão
            CONNECTION_STRING = nl[0]
            # Crie uma conexão usando MongoClient. Você pode importar MongoClient ou usar pymongo.MongoClient
            _client = MongoClient(CONNECTION_STRING)
            # Cria o devolve banco de dados.
            _dbname = _client['projetofinal']
            # o método retorna uma coleção inteira, muita atenção quando ela for muito grande
            return _dbname
        except:
            print('Falha na conexão! Tente novamente')

class SparkG3():

    def iniciar_sessao(self):
        # Importações necessárias para a sessão
        from pyspark.sql.session import SparkSession

        spark = SparkSession.builder.appName('Sessao')\
                                    .config("spark.master", "local")\
                                    .config("spark.executor.memory", "1gb")\
                                    .config("spark.shuffle.sql.partitions", 1)\
                                    .getOrCreate()
        return spark

class UtilidadesG3():

    def converterColuna(self, dataframe, nomes, novoTipo):
        '''
        Método para converter colunas de um dataset de um tipo para outro
        [1º parâmetro] dataFrame, atenção, o dataFrame será modificado
        [2º parâmetro] um array com o nome das colunas
        [3º parâmetro] novo tipo para que a seja efetuada mudança
        '''
        for nome in nomes:
            dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
        return dataframe

    def printp():
        _x = '<|.......................................|>'
        print(_x)

# Código inicial para dataFrame de paises
# path = "C:/scripts/population_csv.csv"
# df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_csv = df_csv.select(['Country Name','Year','Value']).filter(df_csv['Year'] == '2018')

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
# Juntar dois data sets |||||||||||||||| Código da Nicole's bad ||||||||||||||||
#df_atletas_medalhas = df_atletas_medalhas.join(df_medalhas, ["country"], how='left')
df_atletas_medalhas = df_atletas.join(df_medalhas,\
     df_medalhas['namecountry'] == df_atletas['country'], how='left')
# df_atletas_medalhas.show()

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
# df_atletas_medalhas.show()
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

#Tem o Kyrgyzstan no atletas mas não tem no população
# df_populacao = df_populacao.select(['Country Name','Year','Value']).filter(df_populacao['Country Name'] 
#                                     == 'Kyrgyzstan')
# df_atletas_medalhas = df_atletas_medalhas.select('namecountry').filter(df_atletas_medalhas['namecountry'] == 'Kyrgyzstan')
# df_atletas_medalhas.show()

# Criei uma nova lista com o Chinese Taipei
# chinese_list = ['Chinese Taipei', 2018, 23000000]
# turn row into dataframe
# chinese_df = pd.DataFrame(chinese_list)
# print(type(chinese_df))
# union with existing dataframe
# df_populacao2 = df_populacao.union(chinese_df)
# df_populacao2.show(500)

# row3 = ['Chinese Taipei', '2018', '23000000']
# row1 = [0, 2018, 23000000]
# df4 = pd.Series(row1)
# print(df4)
# print(type(df4))
# df5 = pd.DataFrame(df4)
# print(type(df5))
# row2 = spark.createDataFrame(row1)
# print(type(row2))
header = ['country', 'year', 'value']
chinese = [('Chinese Taipei', 2018, 23000000)]

df = spark.createDataFrame(chinese, header)
# print(type(df))                            
# df_populacao.printSchema()
# print(type(df_populacao))
df_populacao = df_populacao.union(df)
df_populacao.show(500)


#df1.filter(df1['Country Name'] == 'United States of America').select('Country Name','Value').show(50)

df_atletas_medalhas_pop = df_atletas_medalhas.join(df_populacao,\
     df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left')
# df_atletas_medalhas_pop.show(500)

# print('...\n\n\n...')

df_pop_atletas_medalhas = df_populacao.join(df_atletas_medalhas, \
    df_populacao['Country Name'] == df_atletas_medalhas['country'], how='right')
#df_pop_atletas_medalhas.show(5)

x = df_atletas_medalhas_pop.select(['country']).count()
y = df_pop_atletas_medalhas.select(['country']).count()
# print ('contagem de linhas \n ',x,'...', y,'...')
df1 = df_pop_atletas_medalhas.groupBy('country','Country name','Value').count().sort('Value').limit(36).toPandas()
#print(df1)
# Mostrando os países (distintos) agrupados por população filter('Value' == 'null')
# print('...\n\n\n...')
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
# print('..........->->->')
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
