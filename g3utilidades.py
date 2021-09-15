class UtilidadesG3():

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