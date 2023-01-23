from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def cria_dataframe(spark, path):
    df = (spark.read
          .option("inferSchema", "true")
          .option("delimiter", ";")
          .option("header", "true")
          .option('encoding', 'utf-8')
          .csv(path))
    return df


def cria_views(df, spark, query):
    df.createOrReplaceTempView("enem")
    df_view = spark.sql(query)
    return df_view


if __name__ == '__main__':
    PATH = '/home/ivan/Xpe-Cursos/Engenharia/microdados_enem_2020/DADOS/MICRODADOS_ENEM_2020.csv'
    spark = spark_session()
    df = cria_dataframe(spark, PATH)
    query = '''
    SELECT 
    MEAN(NU_NOTA_CH) 
    FROM enem 
    WHERE SG_UF_ESC = 'SC'
    AND Q008 = 'B'
    '''
    enem = cria_views(df, spark, query)
    enem.show()
