from pyspark.sql.session import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('firstSeesion')\
    .config("spark.master","local[4]") \
    .config("spark.executor.memory","1gb") \
    .config("spark.shuffle.sql.partitions",2 ) \
    .getOrCreate()

path = 'C:/Users/Ricardo Felipe/Desktop/SoulCode/Engenharia de Dados/Projeto FInal/paralympics_tokyo.json'
df_json = spark.read.option("multiline","true").json(path)
# df_json.show(10)
# df_json.printSchema()
# df_json.select('sex').count() #não retorna nada, converti para Pandas:
#converter para Dataframe do pandas
pandasDf = df_json.toPandas()
print(f'A tabela possui {pandasDf.shape[0]} linhas e {pandasDf.shape[1]} colunas!\n')


path = 'C:/Users/Ricardo Felipe/Desktop/SoulCode/Engenharia de Dados/Projeto FInal/Paralympics_tokyo_21.csv'
df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
# df_csv.show()
# df_csv.printSchema()
df_csv.select('rank').count()

#=================================NÚMERO DE LINHAS DIFERENTES ENTRE O CSV E O JSON, JOIN INPOSSÍVEL==========================

print(pandasDf['rank'].value_counts()) #927 valores únicos, muitos arrays

# df_json = df.drop('name')
# df_csv = df_csv.select('medal', 'name', 'rank')

# df_json.show()
# df_csv.show()

# df_join = df_json.join(df_csv,df_json['rank'] ==  df_csv['rank'])
# df_join.show(20)

# df_join = df_join.toDF(*['Age', 'Nacionalidade', 'Sexo', 'Esporte', 'Seleção', 'Medalha', 'Nome'])
# df_join.printSchema()

# def colunaTipo(dataframe, nomes, novoTipo):
#     for nome in nomes:
#         dataframe = dataframe.withColumn(nome, dataframe[nome].cast(novoTipo))
#     return dataframe

# colunaI = ['Age']

# df_join = colunaTipo(df_join, colunaI, IntegerType())

# df_join.show(10)
# df_join.printSchema()

# df_join = df_join.groupby('E')
