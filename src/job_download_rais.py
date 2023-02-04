import gzip
import os
import ftplib
import shutil
import logging

from pyunpack import Archive

BASENAME = os.path.basename(__file__).replace('.py', '')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    # filename=f'{BASENAME}.log',
    # filemode='a',
    datefmt='%d-%m-%Y %H:%M:%S',
    encoding='utf-8',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'{BASENAME}.log', 'a')
    ]
)


def conection():
    ftp = ftplib.FTP('ftp.mtps.gov.br', "anonymous", 'anonymous@domain.com')
    if not ftp:
        logging.error('Erro ao conectar ao servidor FTP')
        return
    else:
        logging.info('Conetado com Sucesso ao Servidor FTP', exc_info=True)
        return ftp


def get_file(ftp):
    ftp.cwd('/pdet/microdados/RAIS/2020')

    files = ftp.nlst()
    logging.info('Listando os arquivos do diretorio')

    todownload = [x for x in files if x.startswith("RAIS")]
    for filename in todownload:
        logging.info('Diretorio de destino: ./Rais/')
        with open('./Rais/' + filename, "wb") as f:
            logging.info('Iniciando o download dos arquivos')
            ftp.retrbinary('RETR %s' % filename, f.write)
    ftp.quit()


def unzip(dir, dest):
    for file in os.listdir(dir):
        try:
            logging.info(f"Extraindo: {dir + file}")
            Archive(dir + file, ).extractall(dest)
        except ValueError:
            logging.error('Erro ao descompactar os arquivos', exc_info=True)
            return


def compress_file(dir, dest):
    logging.info('Iniciando a compressao!')
    for file in os.listdir(dir):
        logging.info(f'Compactando arquivo: {file}')
        with open(dir + file, 'rb') as fb:
            logging.warning('Grande volume de dados')
            with gzip.open(f'{dest + file}.gz', 'wb') as fw:
                logging.info(f'Escrevendo arquivos: {dest + file}.gz')
                shutil.copyfileobj(fb, fw)


def main():
    # logging.info('Iniciando')
    # ftp = conection()
    # get_file(ftp)
    unzip('./Rais/', './unizips/')
    compress_file('./unizips/', './compress/')


if __name__ == '__main__':
    main()
