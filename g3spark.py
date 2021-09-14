# 13/09/21
# Classe de acesso a sessão pySpark
class SparkG3():
    def iniciar_sessao(self):
        from pyspark.sql.session import SparkSession
        #from pyspark.sql.types import *
        import pyspark.sql.functions as F

        spark = SparkSession.builder.appName('Sessao')\
                                    .config("spark.master", "local")\
                                    .config("spark.executor.memory", "1gb")\
                                    .config("spark.shuffle.sql.partitions", 1)\
                                    .getOrCreate()
        return spark