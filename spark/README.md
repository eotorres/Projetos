Processamento de Dados em Streaming com PySpark

Este projeto demonstra como processar dados em streaming utilizando o PySpark, uma das ferramentas mais robustas para processamentos distribuídos em grandes volumes de dados. O foco do projeto foi configurar um fluxo de dados em streaming, realizar agregações e gravar os resultados no formato Parquet para análises posteriores.

Tecnologias Utilizadas

Python 3.10

PySpark

Hadoop LocalFileSystem

Formato de saída Parquet

Objetivos

Criar um pipeline de processamento de dados em streaming.

Utilizar agregações em dados de entrada em tempo real.

Configurar checkpoints para garantir a consistência e evitar perda de dados.

Otimizar os parâmetros da SparkSession para eficiência em ambiente local.

Estrutura do Projeto

1. Configuração da SparkSession

O projeto inicia configurando a SparkSession, ajustando parâmetros importantes como:

Memória para executor e driver.

Configuração de reutilização de workers.

Timeout para evitar perda de conexões em streams longos.

2. Esquema do DataFrame

Definimos um esquema de entrada para os arquivos CSV:

nome: Tipo String.

salario: Tipo Integer.

departamento: Tipo String.

Isso garante que os dados sejam lidos de forma estruturada e confiável.

3. Leitura do Streaming

A leitura dos arquivos ocorre a partir de um diretório configurado para streaming:

Diretório: D:/Estudos/Projetos/Spark/streaming2

O Spark monitora constantemente o diretório para detectar novos arquivos CSV.

4. Watermark e Timestamp

Para evitar o acúmulo de memória em streams contínuos, aplicamos:

Uma coluna de timestamp usando a função current_timestamp().

Um watermark de 10 minutos para gerenciar eventos atrasados.

5. Agregações

As agregações foram realizadas por departamento, calculando a média salarial em tempo real:

aggregated_df = streaming_df_with_watermark \
    .groupBy("departamento") \
    .agg(avg("salario").alias("media_salario"))

6. Saída dos Dados

Os resultados processados foram gravados no formato Parquet em:

Diretório de saída: D:/Estudos/Projetos/Spark/streaming_output

Diretório de checkpoints: D:/Estudos/Projetos/Spark/checkpoints

7. Execução do Streaming

O stream foi configurado no modo append, garantindo que apenas os novos dados processados sejam adicionados ao destino.

Configuração Local

Para executar o projeto localmente:

Certifique-se de ter o PySpark instalado:

pip install pyspark

Ajuste o caminho do Python no código: Substitua os.environ["PYSPARK_PYTHON"] pelo caminho correto do seu interpretador Python.

Crie os diretórios necessários: Certifique-se de que os diretórios de entrada e saída existem ou serão criados pelo script.

Resultados e Publicação

Este projeto demonstra como aplicar conceitos importantes em PySpark para processar dados em streaming, incluindo:

Leitura de arquivos em tempo real.

Aplicação de agregações com controle de tempo usando watermark.

Escrita eficiente de resultados no formato Parquet.
