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

    def criar_df(coluna1, coluna2, coluna3, parametro1, parametro2, parametro3):
        import pandas as pd
        '''
        Método para criar um dataFrame dinamicamente
        [coluna(s)] parâmetros para o nome das colunas
        [parametro(s)] parâmetros da chave do dataFrame
        '''
        _dataf = pd.DataFrame({coluna1: [f'{parametro1:,}'.replace('.', ',')],
                            coluna2: [f'{parametro2:,}'.replace('.', ',')],
                            coluna3: [f'{parametro3:,}'.replace('.', ',')],
        })
        return _dataf