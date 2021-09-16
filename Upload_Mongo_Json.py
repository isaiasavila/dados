from pyspark.sql.session import SparkSession

#importa tipos de daos
from pyspark.sql.types import(BooleanType, IntegerType, IntegralType, StringType, StructType, TimestampType, ArrayType, FloatType, StructField)

import pyspark.sql.functions as F

spark = SparkSession.builder.appName('meu_etl')\
   .config('spark.master', 'local')\
   .config("spark.executor.memory", "2gb") \
   .config('spark.shuffle.sql.partitions', 2)\
   .getOrCreate() 

schema = StructType([StructField("target", StringType()),
                   StructField("_id", IntegerType()),
                   StructField("date", StringType()),
                   StructField("flag", StringType()),
                   StructField("user", StringType()),
                   StructField("text", StringType()),
                  ])
# FIM DE SSECAO
#----------------------------------------
#EXTRACT
path = 'C:/Sparkscripts/training.1600000.processed.noemoticon.csv'
df = spark.read.format('csv')\
    .schema(schema)\
    .load(path)
#df.printSchema()
#df.show(5)
#----------------------------------------
#TRANSFORM
df = df.drop('target', 'flag')

#Pegar partes das linhas de uma coluna
df = df.withColumn('day_week', df.date.substr(1,3))\
        .withColumn('day', df.date.substr(9,2))\
        .withColumn('month', df.date.substr(5,3))\
        .withColumn('time', df.date.substr(12,8))\
        .withColumn('year', df.date.substr(25,4))\
        .drop('date')
#df.printSchema()
#df.show(5)

#Cria colunas
def converterColuna(dataframe, nomes, novoTipo):
    for nome in nomes: 
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe 

colunas_inteiro = ['day']
df = converterColuna(df,colunas_inteiro,IntegerType())

#df.printSchema()
#df.show(5)

#Load
df = df.limit(20)
#df.write.format('json').save('meuDF2.json')#Forma original do Spark para gerar o arq p/ export
#print('JSON gerado com sucesso')
pandas_df = df.toPandas()
pandas_df.to_json('lu.json', orient='records')#passa df p/pandas e gera o arq p/ exportar
print('JSON gerado com sucesso')

#----------------------------------------
#UPLOAD

def get_database():
    from pymongo import MongoClient
    import pymongo

#URL de conexao   
def get_database():
    from pymongo import MongoClient
    import pymongo

    #URL DECONEXÃO
    CONNECTION_STRING = "mongodb+srv://lu:lcm011@cluster0.xgi78.mongodb.net/test"

    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)

    return client['soulcodeTeste2']

dbname = get_database()
collection_name = dbname["itens_soulcode_x"]

df = df.limit(20)#arquivo gerado em json

df = df.toPandas()#transforma o dataframe em pandas
data_dict = df.to_dict('records')#somente em pandas consigo transformar em dicionário
collection_name.insert_many(data_dict)
print("DF importado")
