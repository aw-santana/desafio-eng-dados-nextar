import logging
import time
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from etl.extract.extract_data import extrair_dados
from etl.transform.transform_data import tratar_dados
from etl.load.load_data import carregar_bigquery
from logs.logging import config_log
from google.cloud import bigquery

load_dotenv()
config_log()  

def main():

    url_empresas = os.getenv("url_empresas")
    url_municipios = os.getenv("url_municipios")
    dataset_id = os.getenv("dataset_id")
    table_id = os.getenv("table_id")
    project_id = os.getenv("project_id")

    spark = (
        SparkSession.builder
        .appName("ETL_Empresas")
        .config("spark.local.dir", "D:/Spark/Temp")
        .config("spark.jars", os.getenv("bigquery_jar"))
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

    logging.info("=" * 50)
    logging.info("INICIANDO PIPELINE...")
    logging.info("=" * 50)

    df_empresas, df_municipios = extrair_dados(spark, url_empresas, url_municipios)
    df_tratado = tratar_dados(df_empresas, df_municipios)
    carregar_bigquery(df_tratado, table_id, project_id, dataset_id)

    logging.info("PIPELINE FINALIZADO COM SUCESSO!")
    logging.info("=" * 50)

if __name__ == "__main__":
    main()
