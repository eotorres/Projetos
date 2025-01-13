
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

# Agrupando por departamento e agregando múltiplas colunas
df_grouped = df.groupBy("departamento").agg(
    F.sum("salario").alias("salario_total"),
    F.avg("salario").alias("salario_medio"),
    F.count("nome").alias("num_funcionarios")
)
df_grouped.show()

# Criando um segundo DataFrame de exemplo
data2 = [("RH", "Recursos Humanos"), ("TI", "Tecnologia"), ("Financeiro", "Financeiro")]
columns2 = ["departamento", "descricao"]
df2 = spark.createDataFrame(data2, columns2)

# Realizando o join entre os DataFrames
df_joined = df.join(df2, "departamento", "left")
df_joined.show()

# Reparticionando o DataFrame em 4 partes
df_repartitioned = df.repartition(4)

# Gravando os dados em formato Parquet
df_repartitioned.write.mode("overwrite").parquet(r"D:\Estudos\Projetos\Spark\dados\dados.parquet")

# Encerrando a SparkSession
spark.stop()
