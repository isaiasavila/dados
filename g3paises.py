#
from g3spark import SparkG3
import pandas as pd

spark = SparkG3().iniciar_sessao()

path = "C:/scripts/population_csv.csv"
df_csv = spark.read.load(path, format = 'csv', sep = ',', inferschema = 'true', header = 'true')
df_csv = df_csv.select(['Country Name','Year','Value']).filter(df_csv['Year'] == '2018')


#df = df_csv.toPandas()
# Guardar a população mundial
#print([df['Country Name'] == 'World'].value,' população mundial')

#df = df.drop(labels=range(0, 46), axis=0)

#x = df['Value'].describe()
#y = df['Value'].value_counts(normalize=True)

#print(x,'\n',y)
#df['Value'].describe()
# Índice 45 é o da população mundial
#350.000.000
print(df)


#df_csv.show(20)
