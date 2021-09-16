from numpy import product
from pandas.core.frame import DataFrame
from pymongo import MongoClient
import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
from config import mongoURI
from g3utilidades import UtilidadesG3

client = MongoClient(mongoURI)
db_name = client['projetofinal']
collection_name = db_name['olimpiadas3']
docs = collection_name.find()
df_olimpiadas = pd.DataFrame(docs)
# print(df_olimpiadas.head(5))


#==============================Manipulação========================================

#Atletas com o maior números de medalhas nas Olimipiadas
print(df_olimpiadas['Atleta'].value_counts().head(10))

#Atletas com o menor números de medalhas nas Olimipiadas
print(df_olimpiadas['Atleta'].value_counts().tail(10))

#Análise da quantidade de medalhas femininas nas Olimipiadas
mulheres = pd.DataFrame({'Ouro': df_olimpiadas[(df_olimpiadas['Gênero'] == 'Female') & (df_olimpiadas['Medalha'] == 'Gold Medal')]['Medalha'].value_counts(),
                         'Prata': df_olimpiadas[(df_olimpiadas['Gênero'] == 'Female') & (df_olimpiadas['Medalha'] == 'Silver Medal')]['Medalha'].value_counts(),
                         'Bronze': df_olimpiadas[(df_olimpiadas['Gênero'] == 'Female') & (df_olimpiadas['Medalha'] == 'Bronze Medal')]['Medalha'].value_counts(),    
})

mulheres.plot.hist(bins=15, edgecolor='black',title='Medalhas Femininas - Olímpicas')
mulheres_O = mulheres['Ouro'][1]
mulheres_P = mulheres['Prata'][2]
mulheres_B = mulheres['Bronze'][0]

#Análise da quantidade de medalhas masculinas nas Olimipiadas
homens = pd.DataFrame({'Ouro': df_olimpiadas[(df_olimpiadas['Gênero'] == 'Male') & (df_olimpiadas['Medalha'] == 'Gold Medal')]['Medalha'].value_counts(),
                         'Prata': df_olimpiadas[(df_olimpiadas['Gênero'] == 'Male') & (df_olimpiadas['Medalha'] == 'Silver Medal')]['Medalha'].value_counts(),
                         'Bronze': df_olimpiadas[(df_olimpiadas['Gênero'] == 'Male') & (df_olimpiadas['Medalha'] == 'Bronze Medal')]['Medalha'].value_counts(),    
})
plt.style.use("ggplot")
homens.plot.hist(title='Medalhas Masculinas - Olímpicas')
homem_O = homens['Ouro'][1]
homem_P = homens['Prata'][2]
homem_B = homens['Bronze'][0]

display(UtilidadesG3.criar_df('Ouro', 'Prata','Bronze',mulheres_O, mulheres_P, mulheres_B).style.hide_index())
display(UtilidadesG3.criar_df('Ouro', 'Prata','Bronze',homem_O, homem_P, homem_B).style.hide_index())

# ano = pd.Series(df["Ano"])
# nq = pd.Series(df["Número de Queimadas"])
# nm = pd.Series(df["Poluição do Ar (mortes por 100,000)"])
# #display(nm)