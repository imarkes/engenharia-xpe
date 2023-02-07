import logging
import os
import pathlib

from pyunpack import Archive
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def spark_session():
    spark = SparkSession.builder.appName('Trabalho pratico modulo 2').getOrCreate()
    return spark


def cria_dataframe(spark, path):
    df = (spark.read
          .option("inferSchema", "true")
          .option("sep", "\t")
          .option("header", "true")
          .option('encoding', 'utf-8')
          .csv(path))
    return df


def cria_views(df, spark, name_view, query):
    df.createOrReplaceTempView(name_view)
    df_view = spark.sql(query)
    return df_view


def unzip(dir, name, dest):
    for file in os.listdir(dir):
        try:
            if name in file:
                logging.info(f"Extraindo: {dir + file}")
                if not os.path.exists(f'{dest}/{name}/'):
                    os.mkdir(f'{dest}/{name}/')
                    logging.info(f'Extraindo dados em: {dest}/{name}/')
                    Archive(dir + file).extractall(f'{dest}/{name}/')
                else:
                    logging.info('Path já existe!')
        except ValueError:
            logging.error('Erro ao descompactar os arquivos', exc_info=True)
            return


def union_dataframes(spark, df1, df2):
    df = df1.union(df2)


if __name__ == '__main__':
    PATH_DATA_UNZIP = '../landing_data/'
    PATH_DATA_CSV = '../landing_data/'

    # unzip files
    unzip_titles = unzip(PATH_DATA_UNZIP, 'basics', PATH_DATA_CSV)
    unzip_ratings = unzip(PATH_DATA_UNZIP, 'ratings', PATH_DATA_CSV)

    # Creating df
    spark = spark_session()
    df_titles = cria_dataframe(spark, '../landing_data/basics/data.tsv')
    df_ratings = cria_dataframe(spark, '../landing_data/ratings/data.tsv')

    # Union dfs
    df = df_titles.join(df_ratings, on='tconst', how='left')
    df.show(5)


    # query = '''
    # SELECT
    # MEAN(NU_NOTA_CH)
    # FROM enem
    # WHERE SG_UF_ESC = 'SC'
    # AND Q008 = 'B'
    # '''
    # name_view = 'imdb'
    # enem = cria_views(df, spark, name_view, query)
    # enem.show()
