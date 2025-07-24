import os
import time
import logging
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def baixar_arquivo(url_empresas: str, url_municipios: str):
    arquivo_empresas = os.path.basename(urlparse(url_empresas).path)
    arquivo_municipios = os.path.basename(urlparse(url_municipios).path)  

    empresa_csv = os.path.join("data","csv", arquivo_empresas)
    municipio_csv = os.path.join("data","csv", arquivo_municipios)

    os.makedirs(os.path.dirname(empresa_csv), exist_ok=True)
    os.makedirs(os.path.dirname(municipio_csv), exist_ok=True)
    
    # Baixar arquivo de empresas
    if not os.path.exists(empresa_csv):
        logging.info(f"--> Baixando arquivos de: {url_empresas}")

        resposta = requests.get(url_empresas, stream=True)
        resposta.raise_for_status()
        inicio = time.time()

        with open(empresa_csv, "wb") as f:
            for chunk in resposta.iter_content(chunk_size=8192):
                f.write(chunk)

        fim = time.time()
        logging.info(f"--> Download concluído: {empresa_csv}. Tempo execução {fim - inicio:.2f} segundos")
    else:
        logging.info(f"--> Arquivo já baixado: {empresa_csv}")
    
    # Baixar arquivo de municípios
    if not os.path.exists(municipio_csv):
        logging.info(f"--> Baixando arquivo de municípios de: {url_municipios}")
        
        resposta = requests.get(url_municipios, stream=True)
        resposta.raise_for_status()
        inicio = time.time()

        with open(municipio_csv, "wb") as f:
            for chunk in resposta.iter_content(chunk_size=8192):
                f.write(chunk)
        
        fim = time.time()
        logging.info(f"--> Download concluído: {municipio_csv}. Tempo de execução: {fim - inicio:.2f} segundos")
    else:
        logging.info(f"--> Arquivo já baixado: {municipio_csv}")

    return empresa_csv, municipio_csv
    

def extrair_dados(spark: SparkSession, url_empresas: str, url_municipios: str):
    logging.info(f"--> Iniciando a extração dos dados...")
    inicio = time.time()

    csv_empresas, csv_municipios = baixar_arquivo(url_empresas, url_municipios)

    # Empresas
    empresas = os.path.splitext(os.path.basename(urlparse(url_empresas).path))[0]
    parquet_empresas = os.path.join("data", "parquet", f"{empresas}.parquet")

    # Schemas
    schema_empresas = StructType([StructField(f"_c{i}", StringType(), True) for i in range(30)])

    if os.path.exists(parquet_empresas):
        df_empresas = spark.read.parquet(parquet_empresas)
    else:
        df_empresas = (spark.read.option("sep", ";")
                               .option("quote", '"')
                               .option("escape", '"')
                               .option("multiLine", True)
                               .option("header", "false")
                               .option("encoding", "latin1")
                               .schema(schema_empresas)
                               .csv(csv_empresas))
        os.makedirs(os.path.dirname(parquet_empresas), exist_ok=True)
        df_empresas.write.mode("overwrite").parquet(parquet_empresas)

    # Municipios
    municipios = os.path.splitext(os.path.basename(urlparse(url_municipios).path))[0]
    parquet_municipios = os.path.join("data", "parquet", f"{municipios}.parquet")

    # Schemas
    schema_municipios = StructType([
        StructField("_c0", StringType(), True),
        StructField("_c1", StringType(), True)
    ])

    if os.path.exists(parquet_municipios):
        df_municipios = spark.read.parquet(parquet_municipios)
    else:
        df_municipios = (spark.read.option("sep", ";")
                                   .option("quote", '"')
                                   .option("escape", '"')
                                   .option("multiLine", True)
                                   .option("header", "false")
                                   .option("encoding", "latin1")
                                   .schema(schema_municipios)
                                   .csv(csv_municipios))
        os.makedirs(os.path.dirname(parquet_municipios), exist_ok=True)
        df_municipios.write.mode("overwrite").parquet(parquet_municipios)

    logging.info(f"--> Finalizando extração...")

    fim = time.time()
    logging.info(f"--> Extração concluída!")
    logging.info(f"--> Empresas: {df_empresas.count():,} registros")
    logging.info(f"--> Municípios: {df_municipios.count():,} registros")
    logging.info(f"--> Tempo total: {fim - inicio:.2f} segundos")
    logging.info("-" * 40)

    return df_empresas, df_municipios
