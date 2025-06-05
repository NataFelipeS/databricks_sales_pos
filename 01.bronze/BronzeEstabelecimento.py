# Databricks notebook source
# Importações
from pyspark.sql.functions import current_date

# COMMAND ----------

# Nome do database e tabela
database = 'bronze'
tabela = 'estabelecimentos'

# Caminho do arquivo
caminho_arquivo = 'dbfs:/FileStore/Sales-pos/sales_5000000.csv'

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(caminho_arquivo)

# COMMAND ----------



# COMMAND ----------

df_com_data = df.withColumn("data_hoje", current_date())

# COMMAND ----------



# COMMAND ----------

df = df.toDF(*[col.strip().replace(" ", "_") for col in df.columns])

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Grava os dados no formato Delta
df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')  
print("Dados gravados com sucesso!") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.estabelecimentos