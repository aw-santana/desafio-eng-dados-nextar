
# Desafio Técnico | Nextar

Este projeto consiste na construção de um pipeline de dados completo para ingestão, transformação e visualização dos dados. 

---

## Objetivo

Criar um pipeline de dados que:
1. Faça **extração** de arquivos CSV públicos no Amazon S3.
2. Realize a **transformação e enriquecimento** dos dados.
3. Carregue os dados tratados no **Google BigQuery**.
4. Exiba visualizações analíticas no **Looker Studio**.

---

## Tecnologias e Ferramentas Utilizadas

- Python 3.10+
- Apache PySpark
- Google BigQuery (conector oficial do Spark)
- Looker Studio (Google Data Studio)
- Google Cloud Storage (BigQuery auth)
- `.env` para variáveis de ambiente
- Logging estruturado com `logging`

---

## Estrutura do Projeto

```
├── etl/
│   ├── extract/
│   │   └── extract_data.py
│   ├── transform/
│   │   └── transform_data.py
│   └── load/
│       └── load_data.py
├── logs/
│   └── logging.py
├── main.py
├── requirements.txt
├── .env
└── README.md
```

---

## Como Executar

1. Instale os requisitos:

```bash
pip install -r requirements.txt
```

2. Configure as variáveis no arquivo `.env`:

```env
url_empresas=https://engenheiro-dados-dados-teste.s3.sa-east-1.amazonaws.com/empresas.csv
url_municipios=https://engenheiro-dados-dados-teste.s3.sa-east-1.amazonaws.com/municpios.csv
project_id=SEU_PROJECT_ID
dataset_id=SEU_DATASET_ID
table_id=SEU_TABLE_ID
bigquery_jar=CAMINHO_PARA_O_JAR_DO_BIGQUERY_CONNECTOR
GOOGLE_APPLICATION_CREDENTIALS=caminho/para/sua/credencial.json
```

3. Execute o pipeline:

```bash
python main.py
```

---

## Entregas Realizadas

- Pipeline de extração de dados dos arquivos CSV públicos.
- Transformações e limpeza dos dados:
  - Normalização de strings.
  - Enriquecimento de município via join.
  - Tratamento de datas, e-mails e logradouro.
- Carga eficiente no BigQuery via Spark.
- Dashboard no Looker Studio com os seguintes insights:

---
## Dashboard - Looker Studio

O dashboard está disponível no link abaixo:

🔗 [Relatório de Empresas - Looker Studio](https://lookerstudio.google.com/reporting/81074d87-82e6-4e26-9894-b5ee2696ef29)

**Visões implementadas:**
- Top 10 municípios com mais empresas.
- Distribuição por situação cadastral.
- Tabela analítica com a quantidade de empresas por município.
- Mapa interativo com distribuição geográfica.
- Filtros dinâmicos por empresa, situação, unidade, município, estado e período.

---

## Exemplo do Dashboard

![Dashboard Looker](./img/dashboard-preview.png)

---

## Desenvolvido Por

**Amim Werbert Silva Santana**  
Engenheiro de Dados  
[LinkedIn](https://www.linkedin.com/in/amim-santana)  
[GitHub](https://github.com/aw-santana)

---
