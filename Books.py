import collections


def get_database():
    from pymongo import MongoClient
    import pymongo

    CONNECTION_STRING = "mongodb+srv://dbLeonardo:Brisingr1**@cluster0.rzcb0.mongodb.net/test"

    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)

    return client['soulcodeTeste2']

def mostraDocumentos():
    dbname = get_database()
    collection_name = dbname["AulaDbImportado"]
    

    

            

#a) Quantos livros possuem número de páginas 0? 
    
    # detalhes_itens = collection_name.find({"pageCount" : "0"})
    # i=0
    # for item in detalhes_itens:
        # i+=1
        # print(i)
    

#b) Quantos livros foram publicados?
    # detalhes_itens = collection_name.find({"status" : "PUBLISH"})
    # i=0
    # for item in detalhes_itens:
    #     i+=1
    #     print(i)

        
#c) Qual o título do livro, cujo ISBN é 1933988924? 
    # detalhes_itens = collection_name.find({"ISBN" : "1933988924"})
    
    #for item in detalhes_itens:
     #   print(item['title'])        
#d) Qual a descrição do livro Machine Learning in Action?
    # detalhes_itens = collection_name.find({"title" : "Machine Learning in Action"})
    # for item in detalhes_itens:
    #     print(item['shortDescription'])

#e) O Livro ArcGIS Web Development foi publicado?
    # detalhes_itens = collection_name.find({"title" : "ArcGIS Web Development"})
    # for item in detalhes_itens:
    #     print(item['status'])

#f) Qual a descrição do livro Secrets of the JavaScript Ninja pBook upgrade? 
    # detalhes_itens = collection_name.find({"title" : "Secrets of the JavaScript Ninja pBook upgrade"})
    # for item in detalhes_itens:
    #     if KeyError:
    #         print("chave ou valor inexistente")
    #     else:
    #         print(item['shortDescription'])
#g) Quantas páginas possui o livro Jess in Action? 
    # detalhes_itens = collection_name.find({"title" : "Jess in Action"})
    # for item in detalhes_itens:
    #     print(item['pageCount'])
#h) Quais são os primeiros três livros da coleção?
    # detalhes_itens = collection_name.find().limit(3)
    # for item in detalhes_itens:
        # print(item['title'])

#i Qual o ID do livro, cujo ISBN é 1930110987? Ele é declarado ou setado pelo MongoDB?
    # detalhes_itens = collection_name.find({"isbn" : "1930110987"})
    # for item in detalhes_itens:
        # print(item['_id']))

#1) Quantos livros foram pubicados em 2011?
    # detalhes_itens = collection_name.find({"publishedDate" : "2011-12-12"})
    # for item in detalhes_itens:
        # print(item['title'])
    #2 Qual título tem o _id 21?
    detalhes_itens = collection_name.find({"_id" : "21"})
    for item in detalhes_itens:
        print(item['title'])
mostraDocumentos()