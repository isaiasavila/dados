from pyspark.sql.session import SparkSession

#importa tipos de daos
from pyspark.sql.types import(BooleanType, IntegerType, IntegralType, StringType, StructType, TimestampType, ArrayType, FloatType, StructField)

from pyspark.sql.functions import lit

import pyspark.sql.functions as F

#Configu
spark = SparkSession. builder.appName('TFD') \
    .config('spark.master', 'local') \
    .config('spark.executor.memory', '1gb') \
    .config('spark.shuffle.sql.partitions', 1)\
    .getOrCreate()


schema = StructType([StructField('school', StringType()),
                    StructField('sex', StringType()),
                    StructField('age', IntegerType()),
                    StructField('address', StringType()),
                    StructField('famsize', StringType()),
                    StructField('pstatus', StringType()),
                    StructField('medu', IntegerType()),
                    StructField('fedu', IntegerType()),
                    StructField('mjob', StringType()),
                    StructField('fjob', StringType()),                   
                    StructField('reason', StringType()), 
                    StructField('guardian', StringType()), 
                    StructField('traveltime', IntegerType()),
                    StructField('studytime', IntegerType()),                   
                    StructField('failures', IntegerType()), 
                    StructField('schoolsup', BooleanType()),
                    StructField('famsup', BooleanType()),
                    StructField('paid', BooleanType()),
                    StructField('activities', BooleanType()),
                    StructField('nursery', BooleanType()),
                    StructField('higher', BooleanType()),
                    StructField('internet', BooleanType()),
                    StructField('romantic', BooleanType()),
                    StructField('famrel', IntegerType()),
                    StructField('freetime', IntegerType()),
                    StructField('goout', IntegerType()),
                    StructField('dalc', IntegerType()),
                    StructField('walc', IntegerType()),
                    StructField('health', IntegerType()),
                    StructField('absences', IntegerType()),
                    StructField('g1', IntegerType()),
                    StructField('g2', IntegerType()),
                    StructField('g3', IntegerType()),
])

path = 'C:\Sparkscripts\student_mat.csv'

matematicaDF = spark.read.format('csv') \
    .schema(schema) \
    .load(path, sep=",", header=True)
#df.printSchema()
#df.show(5)

matematicaDF = matematicaDF.withColumn('Subject', lit("Math")) # Cria uma coluna adicionando o subject Math nesse dataFrame

path = 'C:\Sparkscripts\student_por.csv'

portuguesDF = spark.read.format('csv') \
    .schema(schema) \
    .load(path, sep=",", header=True) 

portuguesDF = portuguesDF.withColumn('Subject', lit("Portuguese"))
#df1 = df.limit()
portuguesDF.select('school').count()

novoDF = matematicaDF.union(portuguesDF)

novoDF.groupby('Subject').count().show()

print(novoDF[0][1])