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


