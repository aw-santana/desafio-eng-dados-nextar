from pyspark.sql.functions import *
import logging
import time

def normalizar_texto(df, colunas):
    for coluna in colunas:
        df = df.withColumn(coluna, upper(trim(col(coluna))))
        df = df.withColumn(coluna, translate(
            col(coluna),
            "ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ",
            "AAAAAEEEEIIIIOOOOOUUUUC"
        ))
        df = df.withColumn(coluna, regexp_replace(col(coluna), r"[^A-Z0-9\s]", ""))
        df = df.withColumn(coluna, regexp_replace(col(coluna), r"\s+", " "))
        df = df.withColumn(coluna, when(col(coluna).rlike(r"^\d+\s"), regexp_replace(col(coluna), r"^\d+\s*", "")).otherwise(col(coluna)))
        df = df.withColumn(coluna, when(length(col(coluna)) < 3, None).otherwise(col(coluna)))
        df = df.withColumn(coluna, when(col(coluna).isNull() | (col(coluna) == ""), "NAO INFORMADO").otherwise(col(coluna).cast("string")))
    return df

def tratar_dados(df_empresas, df_municipios):
    logging.info("--> Iniciando tratamento de dados...")
    inicio = time.time()

    colunas = [
        "basico", "ordem", "dv", "unidade", "empresa", "sit_cadastral",
        "dt_sit_cadastral", "mot_sit_cadastral", "cidade_ext", "pais", "dt_inicio",
        "cnae_principal", "cnae_secundario", "tp_logradouro", "logradouro", "numero",
        "complemento", "bairro", "cep", "uf", "municipio", "ddd_telefone_1",
        "telefone_1", "ddd_telefone_2", "telefone_2", "ddd_fax", "telefone_fax",
        "email", "sit_especial", "dt_sit_especial"
    ]
    df = df_empresas.toDF(*colunas)

    # Tratamento CNPJ
    df = df.withColumn("cnpj", concat_ws("",
        lpad(col("basico"), 8, "0"),
        lpad(col("ordem"), 4, "0"),
        lpad(col("dv"), 2, "0")
    )).drop("basico", "ordem", "dv")

    colunas_ordenadas = ["cnpj"] + [c for c in df.columns if c != "cnpj"]
    df = df.select(colunas_ordenadas)

    # Tratamento Datas
    datas = ["dt_sit_cadastral", "dt_inicio", "dt_sit_especial"]

    for coluna in datas:
        df = df.withColumn(coluna, when(col(coluna) == "0", None).otherwise(col(coluna)))
        df = df.withColumn(coluna, to_date(col(coluna), "yyyyMMdd"))

    # Normalização de texto
    colunas_texto_1 = ["empresa", "logradouro", "complemento", "bairro", "tp_logradouro", "cidade_ext", "pais", "sit_especial"]
    df = normalizar_texto(df, colunas_texto_1)

    # Tratamento logradouro
    df = df.withColumn("logradouro", regexp_replace(col("logradouro"), r"^\d+\s*", ""))
    df = df.withColumn("logradouro", regexp_replace(col("logradouro"), r"\b(LOTE|LT|QD|QUADRA|ZERO|CASA|NUMERO|NÚMERO|NO|Nº|SG|UB)\b.*", ""))
    df = df.withColumn("logradouro", trim(regexp_replace(col("logradouro"), r"^0+\s*", "")))
    df = df.withColumn("logradouro", when(col("logradouro").isNull() | (col("logradouro") == ""), "NAO INFORMADO").otherwise(col("logradouro")))

    # Normalizar Nulos
    colunas_texto = [
        "cnae_secundario", "telefone_1", "telefone_2", "telefone_fax", 
        "ddd_telefone_1", "ddd_telefone_2", "ddd_fax", "cep"
    ]
    for coluna in colunas_texto:
        df = df.withColumn(coluna, when(col(coluna) == "0", None).otherwise(col(coluna)))
        df = df.withColumn(coluna, when(col(coluna).isNull() | (col(coluna) == ""), lit("NAO INFORMADO")).otherwise(upper(trim(col(coluna).cast("string"))))
        )

    # Padronização numero
    df = df.withColumn("numero", when(col("numero").rlike("^\d+$"), col("numero")).otherwise("S/N"))

    # Padronização e-mail
    df = df.withColumn("email", when(col("email").isNull() | (col("email") == ""), lit("NAO INFORMADO")).otherwise(lower(trim(col("email")))))

    # Tratamento unidade
    df = df.withColumn("unidade", 
        when(col("unidade") == "1", "MATRIZ")
        .when(col("unidade") == "2", "FILIAL")
        .otherwise(col("unidade"))
    )

    # Tratamento situação cadastral
    df = df.withColumn("sit_cadastral",
        when(col("sit_cadastral") == "01", "NULA")
       .when(col("sit_cadastral") == "02", "ATIVA")
       .when(col("sit_cadastral") == "03", "SUSPENSA")
       .when(col("sit_cadastral") == "04", "INAPTA")
       .when(col("sit_cadastral") == "08", "BAIXADA")
       .otherwise(col("sit_cadastral"))
    )

    # Tratamento municipios
    df = df.join(df_municipios, df["municipio"] == df_municipios["_c0"], "left") \
           .drop("_c0") \
           .drop("municipio") \
           .withColumnRenamed("_c1", "municipio")
    df = df.withColumn("municipio", when(col("municipio").isNull() | (col("municipio") == ""), lit("NAO INFORMADO")).otherwise(upper(trim(col("municipio").cast("string")))))

    fim = time.time()
    logging.info(f"--> Tratamento finalizado! Tempo: {fim - inicio:.2f} segundos")
    logging.info("-" * 40)

    return df
