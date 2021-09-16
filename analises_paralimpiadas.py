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
collection_name = db_name['paraolimpiadas2']
docs = collection_name.find()
df = pd.DataFrame(docs)
# print(df)


#==============================Manipulação========================================

#Atletas com o maior números de medalhas nas paralimpiadas
print(df['Atleta'].value_counts().head(10))

#Atletas com o menor números de medalhas nas paralimpiadas
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
display(UtilidadesG3.criar_df('Ouro', 'Prata','Bronze',mulheres_O, mulheres_P, mulheres_B).style.hide_index())

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
display(UtilidadesG3.criar_df('Ouro', 'Prata','Bronze',homem_O, homem_P, homem_B).style.hide_index())


#Soma do total de medalhas mulheres e homens
soma_mulheres = mulheres_O + mulheres_P + mulheres_B
print(soma_mulheres)

soma_homens = homem_O + homem_P + homem_B
print(soma_homens) 

#Modalidades diferentes de esportes
detalhes_itens = collection_name.distinct("Modalidade")
#display(detalhes_itens)

#print(df[(df["Idade"] > 60)])
# print(df[(df["Idade"] < 15)])