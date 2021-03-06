# Trabalho Grupo 3
# IMPORT
###################################################################################################################
from datetime import date
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd
from pandas.core.frame import DataFrame
import matplotlib.pyplot as plt
from IPython.display import display
from numpy import product
###################################################################################################################
# CLASS
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
    # Classe com métodos para utilidades gerais
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

    def printp(self):
        _x = '<|.......................................|>'
        print(_x)

    def criar_df(self, coluna1, coluna2, coluna3, parametro1, parametro2, parametro3):
        '''
        Método para criar um dataFrame dinamicamente
        [coluna(s)] parâmetros para o nome das colunas
        [parametro(s)] parâmetros da chave do dataFrame
        '''
        _dataf = pd.DataFrame({coluna1: [f'{parametro1:,}'.replace('.', ',')],
                            coluna2: [f'{parametro2:,}'.replace('.', ',')],
                            coluna3: [f'{parametro3:,}'.replace('.', ',')],
        })
        return _dataf

    def mostrar_df(self, _dataFrame):
        _dataFrame.show(25)

    def contar_linha(self, _dataFrame, coluna):
        contador = _dataFrame.select(coluna).count()
        print('número de linhas: ', contador)
        _dataFrame.show(25)
        print('Tipo: ', type(_dataFrame))
        print('\n+.......................................+')

    def mostrar_tipo(self, _dataFrame):
        print(type(_dataFrame))

    def controle_fluxo(s = 0):
        import os
        '''
        Método para segurar ou não a informação na tela do terminal
        '''
        if s == 0:
            input('<enter>')
        else:
            print('<enter>')
        os.system('cls')

###################################################################################################################
# EXTRACT
# Objeto spark criado e o método para criar uma sessão é chamado
spark = SparkG3().iniciar_sessao()
# Criação do objeto de utilidades
util = UtilidadesG3()
# Caminho do 1º arquivo que será utilizado JSON
path = 'C:/scripts/paralympics_tokyo.json'
# Leitura do arquivo em modo multi-linhas
df_para_json = spark.read.option("multiline","true").json(path)
# Caminho do 2º arquivo que será utilizado para o JOIN com o arquivo JSON
path = 'C:/scripts/Paralympics_tokyo_21.csv'
# Leitura do arquivo
df_para_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# Para atender a um dos requisitos do trabalho
# Recebe a quantidade de medalhas ("criando uma chave primária") e ranking para a segregação abaixo
df_para_csv = df_para_csv.groupby('rank', 'medal').count()
# O array de ranking, teve que ser segregado (EXPLODE) para deixar o JSON e CSV prontos para o JOIN
df_explode = df_para_json.select('*', explode_outer(df_para_json.rank).alias('Ranking'))
# Um novo dataFrame com dados da base de paralimpiadas, unificando o JSON e CSV
df_para = df_explode.join(df_para_csv, df_explode.Ranking == df_para_csv.rank, 'left')
# Métodos para testes
# util.mostrar_tipo(df_para)
# util.mostrar_df(df_para)
# util.contar_linha(df_para,'age') # print(z) #7917
# Renomeação das colunas
df_para = df_para.toDF(*['Idade', 'Categoria', 'Nome', 'Nac', 'rank', 'Gênero', 'Esporte', 'Seleção', 'Ranking', 'rank1'
                        , 'Medalha', 'count'])
# DROP de colunas dos dados irrelevantes
df_para = df_para.drop('rank', 'rank1', 'count')
# Caminho do 3º arquivo que será utilizado
path = "C:/scripts/population_csv.csv"
# Leitura do arquivo
df_pop = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# Seleciona os dados do último censo (2018)
df_pop = df_pop.select(['Country Name','Year','Value']).filter(df_pop['Year'] == '2018')
# Efetuado o JOIN utilizando a chave primária (Country Name)
# util.contar_linha(df_pop, 'Country Name') # Teste
df_para_pop = df_para.join(df_pop, df_para.Seleção == df_pop['Country Name'], 'left')
# util.contar_linha(df_para_pop, 'Country Name') # Teste de contagem
# Utilizamos o drop para excluir os países sem medalha
df_para_pop = df_para_pop.dropna(how='any')
# util.contar_linha(df_para_pop, 'Country Name') # Teste de contagem
# df_para_pop.show() # Teste
# Renomeando as colunas para facilitar na identificação dos dados
df_para_pop = df_para_pop.toDF(*['Idade', 'Evento', 'Atleta', 'Nac', 'Gênero', 'Modalidade', 'País', 'Ranking', 'Medalha'
                        , 'country', 'year', 'População_Total'])
# Seleção no formato desejado para as análises
df_para_pop = df_para_pop.select('Atleta', 'Idade', 'Gênero', 'Modalidade', 'Medalha', 'País', 'População_Total')
# Métodos para testes
# util.mostrar_tipo(df_para)
# util.mostrar_df(df_para)
# Lista com o nome das colunas a terem seu tipo alterado
colunasI = ['Idade', 'População_Total']
# Alteração dos tipos de colunas
df_para_pop = util.converterColuna(df_para_pop, colunasI, IntegerType())
# DataFrame tratado
# util.mostrar_df(df_para_pop) # Teste

###################################################################################################################
# EXTRACT
# Olimpíadas (athletes x medals)
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
# Caminho do 1º arquivo que será utilizado
path = "C:/scripts/athletes.csv"
# Parâmetros de conexão
# df1 e df2 serão usados como dataFrames temporários
df_atletas = spark.read.format("csv") \
    .schema(schema) \
    .load(path, sep=",", header=True)
# Alterando o nome de uma das colunas, ação necessária para drop posterior
df_atletas = df_atletas.withColumnRenamed("discipline", "disciplina")
# Caminho do 2º arquivo que será utilizado
path = "C:/scripts/medals.csv"
# Iniciando segunda base de dados
df_medalhas = spark.read.load(path, format="csv", sep=",", inferSchema="true", header="true")
# Alterando o nome de uma das colunas do outro dataset, ação necessária para drop posterior
df_medalhas = df_medalhas.withColumnRenamed("country", "namecountry")
# Juntar dois data sets (JOIN)
df_atletas_medalhas = df_medalhas.join(df_atletas,\
     df_medalhas['namecountry'] == df_atletas['country'], how='left')
# df_atletas_medalhas.show() # Teste de impressão
# Excluindo (DROP) colunas que não serão utilizadas nas análises
df_atletas_medalhas =  df_atletas_medalhas.drop("disciplina", "nome", "medal_code", "medal_date", "athlete_short_name",\
                "athlete_sex", "athlete_link", "country_code", "discipline_code", "short_name",\
                "birth_place", "birth_country", "discipline_code", "residence_place", "residence_country",\
                "height_m/ft", "url")
# Criando uma nova coluna para trazer a idade atual, excluindo a que não será utilizada
df_atletas_medalhas = df_atletas_medalhas.withColumn('age',2021 - df_atletas_medalhas.birth_date.substr(1, 4))\
         .drop('birth_date')
# Eliminando valores que não tenham número
#util.contar_linha(df_atletas_medalhas,'athlete_name')
df_atletas_medalhas = df_atletas_medalhas.dropna(how='any')
# util.mostrar_tipo(df_atletas_medalhas) # Teste
# util.mostrar_df(df_atletas_medalhas) # Teste
# Lista das colunas que serão alteradas
colunas_inteiro = ['age']
# Chamada do método que fará a alteração na coluna
df_atletas_medalhas = util.converterColuna(df_atletas_medalhas, colunas_inteiro, IntegerType())
# Caminho do 3º arquivo que será utilizado
path = "C:/scripts/population_csv.csv"
# github = 'https://raw.githubusercontent.com/isaiasavila/dados/main/population_csv.csv'
# Leitura do arquivo
df_populacao = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# Pandas
# dfp = pd.read_csv('https://raw.githubusercontent.com/isaiasavila/dados/main/population_csv.csv')
# Seleção dos registros do dataset de população
df_populacao = df_populacao.select(['Country Name','Year','Value']).filter(df_populacao['Year'] == '2018')
# Alteração das colunas para Integer
colunas_inteiro = ['Year', 'Value']
# Método da classe para alteração de tipo de coluna
df_populacao = util.converterColuna(df_populacao, colunas_inteiro, IntegerType())
# JOIN da tabela (athletes x medals) x (população)
df_atletas_medalhas_pop = df_atletas_medalhas.join(df_populacao,\
     df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left')
#util.contar_linha(df_atletas_medalhas_pop,'Country Name') # Teste
# Seleção final dos dados que serão trabalhados
df_atletas_medalhas_pop = df_atletas_medalhas_pop.select(['athlete_name','age','gender',\
                                                          'discipline','medal_type','country','value'])
# Excusão dos valores não numéricos
df_atletas_medalhas_pop = df_atletas_medalhas_pop.dropna()
# Excusão dos valores duplicados
df_atletas_medalhas_pop = df_atletas_medalhas_pop.drop_duplicates()
# Renomeação dos cabeçalhos para o português
df_atletas_medalhas_pop = df_atletas_medalhas_pop.toDF(*['Atleta', 'Idade', 'Gênero', 'Modalidade',\
                                                         'Medalha', 'País', 'População_Total'])
# util.contar_linha(df_atletas_medalhas_pop,'Atleta') # Teste
###################################################################################################################
# TRANSFORM/ ANALYSIS - NICOLE - OLIMPÍADAS
###################################################################################################################
# Transformando o Dataset em um dataFrame Pandas
# df_atletas_medalhas_pop = df_atletas_medalhas_pop.toPandas()
# # Atletas com o maior números de medalhas nas Olimpíadas
# print(df_atletas_medalhas_pop['Atleta'].value_counts().head(10))
# # Atletas com o menor números de medalhas nas Olimpíadas

# #util.controle_fluxo()
# print(df_atletas_medalhas_pop['Atleta'].value_counts().tail(10))
# #util.controle_fluxo()
# # Análise da quantidade de medalhas femininas nas Olimpíadas
# mulheres = pd.DataFrame({'Ouro': df_atletas_medalhas_pop[(df_atletas_medalhas_pop['Gênero'] == 'Female') &\
#                          (df_atletas_medalhas_pop['Medalha'] == 'Gold Medal')]['Medalha'].value_counts(),\
#                          'Prata': df_atletas_medalhas_pop[(df_atletas_medalhas_pop['Gênero'] == 'Female') &\
#                          (df_atletas_medalhas_pop['Medalha'] == 'Silver Medal')]['Medalha'].value_counts(),\
#                          'Bronze': df_atletas_medalhas_pop[(df_atletas_medalhas_pop['Gênero'] == 'Female') &\
#                          (df_atletas_medalhas_pop['Medalha'] == 'Bronze Medal')]['Medalha'].value_counts(),    
# })
# # Escolha do estilo de gráfico
# plt.style.use("ggplot")
# # Gráfico de 
# #mulheres.plot.hist(bins=15, edgecolor='black',title='Medalhas Femininas - Olímpicas')
# # Seleção das quantias de medalhas
# mulheres_O = mulheres['Ouro'][1]
# mulheres_P = mulheres['Prata'][2]
# mulheres_B = mulheres['Bronze'][0]
# #Análise da quantidade de medalhas masculinas nas Olimpíadas
# homens = pd.DataFrame({'Ouro': df_atletas_medalhas_pop[(df_atletas_medalhas_pop['Gênero'] == 'Male') &\
#                      (df_atletas_medalhas_pop['Medalha'] == 'Gold Medal')]['Medalha'].value_counts(),\
#                          'Prata': df_atletas_medalhas_pop[(df_atletas_medalhas_pop['Gênero'] == 'Male') &\
#                      (df_atletas_medalhas_pop['Medalha'] == 'Silver Medal')]['Medalha'].value_counts(),\
#                          'Bronze': df_atletas_medalhas_pop[(df_atletas_medalhas_pop['Gênero'] == 'Male') &\
#                      (df_atletas_medalhas_pop['Medalha'] == 'Bronze Medal')]['Medalha'].value_counts(),    
# })
# # Gráfico de 
# #homens.plot.hist(title='Medalhas Masculinas - Olímpicas')
# # Seleção das quantias de medalhas
# homem_O = homens['Ouro'][1]
# homem_P = homens['Prata'][2]
# homem_B = homens['Bronze'][0]
# # Impressão de medalhas femininas
# print('Medalhas femininas...')
# display(util.criar_df('Ouro', 'Prata','Bronze',mulheres_O, mulheres_P, mulheres_B)) # .style.hide_index()
# #util.controle_fluxo()
# # Impressão de medalhas masculinas
# print('Medalhas masculinas...')
# display(util.criar_df('Ouro', 'Prata','Bronze',homem_O, homem_P, homem_B)) # .style.hide_index()
# #util.controle_fluxo()
# input('<enter>')

###################################################################################################################
# TRANSFORM/ ANALYSIS - LEONARDO - OLIMPÍADAS
###################################################################################################################
# Transformando o Dataset em um dataFrame Pandas
df = df_para_pop.toPandas()
# Atletas com o maior números de medalhas nas paralimpiadas
print('Atletas com o maior números de medalhas nas paralimpiadas')
print(df['Atleta'].value_counts().head(10))
#Atletas com o menor números de medalhas nas paralimpiadas
print('Atletas com o menor números de medalhas nas paralimpiadas')
print(df['Atleta'].value_counts().tail(10))

#Análise da quantidade de medalhas femininas nas paralimpiadas
mulheres = pd.DataFrame({'Ouro': df[(df['Gênero'] == 'Female') & (df['Medalha'] == 'Gold')]['Medalha'].value_counts(),
                         'Prata': df[(df['Gênero'] == 'Female') & (df['Medalha'] == 'Silver')]['Medalha'].value_counts(),
                         'Bronze': df[(df['Gênero'] == 'Female') & (df['Medalha'] == 'Bronze')]['Medalha'].value_counts(),    
})
mulheres_O = mulheres['Ouro'][1]
mulheres_P = mulheres['Prata'][2]
mulheres_B = mulheres['Bronze'][0]
mulheres.plot.bar(title='Medalhas Femininas - Paralímpicas')
print('Medalhas femininas...')
display(util.criar_df('Ouro', 'Prata','Bronze',mulheres_O, mulheres_P, mulheres_B))

#Análise da quantidade de medalhas masculinas nas paralimpiadas
homens = pd.DataFrame({'Ouro': df[(df['Gênero'] == 'Male') & (df['Medalha'] == 'Gold')]['Medalha'].value_counts(),
                         'Prata': df[(df['Gênero'] == 'Male') & (df['Medalha'] == 'Silver')]['Medalha'].value_counts(),
                         'Bronze': df[(df['Gênero'] == 'Male') & (df['Medalha'] == 'Bronze')]['Medalha'].value_counts(),    
})
homem_O = homens['Ouro'][1]
homem_P = homens['Prata'][2]
homem_B = homens['Bronze'][0]
plt.style.use("fivethirtyeight")
homens.plot.barh(title='Medalhas Masculinas - Paralímpicas')
print('Medalhas masculinas...')
display(util.criar_df('Ouro', 'Prata','Bronze',homem_O, homem_P, homem_B))


#Soma do total de medalhas mulheres e homens
print('Soma total de medalhas femininas...')
soma_mulheres = mulheres_O + mulheres_P + mulheres_B
print(soma_mulheres)
print('Soma total de medalhas masculinas...')
soma_homens = homem_O + homem_P + homem_B
print(soma_homens) 

#Modalidades diferentes de esportes
#serie = pd.Series.unique(['Esporte'])
#detalhes_itens = df.distinct("Esporte")

###################################################################################################################
# GRAPHICS - RODRIGO - OLIMPÍADAS
###################################################################################################################
















# Códigos de testes ##########################################################################################

# df_pop_atletas_medalhas = df_populacao.join(df_atletas_medalhas, \
#     df_populacao['Country Name'] == df_atletas_medalhas['country'], how='right')
# #df_pop_atletas_medalhas.show(5)

# x = df_atletas_medalhas_pop.select(['country']).count()
# y = df_pop_atletas_medalhas.select(['country']).count()
# # print ('contagem de linhas \n ',x,'...', y,'...')
# df1 = df_pop_atletas_medalhas.groupBy('country','Country name','Value').count().sort('Value').limit(36).toPandas()

# df_populacao.show(500000)
# print(df_populacao)
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


# y = ['country', 'year', 'value']
# x = [('Chinese Taipei', 2018, 23000000)]
# z = ['year', 'value']
# ets = spark.createDataFrame(x,y)#chinese, header)
# ets = util.converterColuna(ets,z,IntegerType())
# print(type(ets))
# input('vai gerar o erro!')
#ets.show()

# print(type(df))                            
# df_populacao.printSchema()
# print(type(df_populacao))
#df_populacao = df_populacao.union(ets)
# df_populacao.show(500) # Teste de impressão
# print('.....|||.....|||.....|||')
# df_populacao.filter(df_populacao['Country Name'] == 'Chinese Taipei').show()
# print('.....|||.....|||.....|||')
# x = df_populacao.select(['country']).count()

# print(x)


#df1.filter(df1['Country Name'] == 'United States of America').select('Country Name','Value').show(50)
#print(df1) # Teste de impressão
# Mostrando os países (distintos) agrupados por população filter('Value' == 'null')

# Visualizando 
#df1 = df_atletas_medalhas_pop.groupby('country', 'Value').count().filter(df_atletas_medalhas_pop['country'] != 'Argentina')
#df1 = df_atletas_medalhas_pop.groupby('country', 'Value').count().filter(df_atletas_medalhas_pop['Value'] )

# print('...\n\n\n...')
# print('..........->->->')
# |||||||||||||||| Código da Nicole's bad ||||||||||||||||
#df_atletas_medalhas = df_atletas_medalhas.join(df_medalhas, ["country"], how='left')
# x = df_atletas_medalhas.select(['country']).count()
# y = df_populacao.select(['Country Name']).count()
# z = df_atletas_medalhas_pop.select(['Country Name']).count()
# print ('contagem de linhas \n ',x,'...', y,'...', z)
#df_atletas_medalhas_pop.show(100)
# df_atletas_medalhas_pop = df_populacao.join(df_atletas_medalhas, df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left')
# df_atletas_medalhas_pop.show(20)
#df_atletas_medalhas.join(df_atletas_medalhas, df_populacao['Country Name'] == df_atletas_medalhas['country'], how='left').show()
# df_atletas_medalhas_pop.show(200)
# Código inicial para dataFrame de paises
# path = "C:/scripts/population_csv.csv"
# df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_csv = df_csv.select(['Country Name','Year','Value']).filter(df_csv['Year'] == '2018')

#Ricardo
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