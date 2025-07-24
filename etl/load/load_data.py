import logging
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StringType, DateType
import time


def carregar_bigquery(df, table_id, project_id, dataset_id):
    logging.info(f"--> Carregando dados para a tabela: {project_id}.{dataset_id}.{table_id}")
    inicio = time.time()

    # Conversão datas
    df = df.select([col(c).cast(DateType()) if "dt_" in c else col(c).cast("string")for c in df.columns])
    df = df.withColumn("dt_ingestao", current_timestamp())
    df = df.repartition(10)

    (
        df.write.format("bigquery")
        .option("table", f"{project_id}:{dataset_id}.{table_id}")
        .option("writeMethod", "direct")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .mode("overwrite")
        .save()
    )

    logging.info(f"--> Dados carregados com sucesso: {df.count()} registros.")
    logging.info("=" * 50)