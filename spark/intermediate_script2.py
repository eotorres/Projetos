
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import IntegerType
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Programas\Python310\python.exe"  # Substitua pelo caminho correto


# Criar a SparkSession
spark = SparkSession.builder \
    .appName("Processamento Intermediario") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .getOrCreate()

# Configurações para evitar mensagens de erro e advertência
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
spark.conf.set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Ler arquivo CSV
input_path = "D:/Estudos/Projetos/Spark/exemplo1.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Substituir valores nulos na coluna 'salario' com 0
df = df.fillna({"salario": 0})

# Remover linhas com valores nulos em qualquer coluna
df_cleaned = df.dropna()

# 2.2 Usar UDF para transformação personalizada
# Criar uma UDF que aplica um bônus baseado no salário
def calcular_bonus(salario):
    if salario > 5000:
        return salario * 0.1
    return salario * 0.05

# Registrar a UDF
bonus_udf = udf(calcular_bonus, IntegerType())

# Aplicar a UDF para adicionar a coluna de bônus
df = df.withColumn("bonus", bonus_udf(col("salario")))

# 2.3 Particionar os dados por departamento para otimizar leitura/gravação
output_path = "D:/Estudos/Projetos/Spark/dados_particionados"
df.write.partitionBy("departamento").parquet(output_path, mode="overwrite")

# Mostrar os dados limpos e com a transformação
df_cleaned.show()

# Encerrando a SparkSession
spark.stop()