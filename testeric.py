#======================================================INICIO DE SESSÃO=======================================================================
import findspark
from traitlets.traitlets import Integer
findspark.init() # buscará nesta pasta: c:\spark-3.1.2-bin-hadoop2.7

from pyspark.sql.session import SparkSession

from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType, TimestampType, StructType, 
StructField, ArrayType, FloatType) #aqui optei por importar apenas os tipos que seriam utilizados;
#acima podemos colocar o * para selecionar todos os tipos;

import pyspark.sql.functions as Func # funções que serão utilizadas no tratamento (?) dos dados

spark = SparkSession.builder.appName('zomato') \
    .config('spark.master', 'local') \
    .config('spark.executor.memory', '2gb') \
    .config('spark.shuffle.sql.partitions', 2) \
    .getOrCreate()


sparkC = spark.sparkContext

#============================================================EXTRACT=========================================================================

df = spark.read.load('C:\script\zomato.csv', format = 'csv', sep = ',', inferschema = 'true', header = 'true')
#puxei esse só para ter um df e um schema setado, para poder dar o drop e depois alterar o tipo com o withColumn();

#df.printSchema() #inferschema deu errado;

# schema = StructType([StructField('Restaurant Name', StringType()),
#                     StructField('Country Code', IntegerType()),
#                     StructField('City', StringType()),
#                     StructField('Locality', StringType()),
#                     StructField('Cuisines', StringType()),
#                     StructField('Average Cost for two', FloatType()),
#                     StructField('Has Online delivery', StringType()),
#                     StructField('Is delivering now', StringType()),
#                     StructField('Price range', IntegerType()),
#                     StructField('Aggregate rating', FloatType()),
#                     StructField('Rating color', StringType()),
#                     StructField('Rating text', StringType()),
#                     StructField('Votes', IntegerType())
# ])

# path = 'C:\script\zomato.csv'

# df = spark.read.format('csv') \
#     .schema(schema) \
#     .load(path)

# precisamos tirar algumas colunas e testamos com esse schema e não funcionou. Faremos usando o withColumn;
df = df.drop('Restaurant ID', 'Address', 'Locality Verbose', 'Longitude', 'Latitude', 'Currency', 'Has Table booking', 
'Switch to order menu')

#df.printSchema()

def colunaTipo(dataframe, nomes, novoTipo):
    for nome in nomes:
        dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
    return dataframe

colunasF = ['Average Cost for two', 'Aggregate rating']
colunasI = ['Country Code', 'Price range', 'Votes']

df = colunaTipo(df, colunasF, FloatType())
df = colunaTipo(df, colunasI, IntegerType())


df.printSchema()
df.show(5)

#converter para Dataframe do pandas
pandasDf = df.toPandas()
print((type(pandasDf))) # tipo retornado: <class 'pandas.core.frame.DataFrame'> 
#value_counts() do pandas
print(pandasDf['City'].value_counts())
print(f'A tabela possui {pandasDf.shape[0]} linhas e {pandasDf.shape[1]} colunas!\n')
