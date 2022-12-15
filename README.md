# ProjectCovid19
## Descrição do Projeto

Este repositório tratará de um projeto de engenheria de dados pensado pela Semantix como parte final e facultativa do curso Big Data Engineer. 

## Etapas do projeto

1. Enviar os dados para o hdfs
2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.
3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS: 
    - CASOS CONFIRMADOS
    - CASOS RECUPERADOS
    - ÓBITOS CONFIRMADOS
4. Salvar a primeira visualização como tabela Hive
5. Salvar a segunda visualização com formato parquet e compressão snappy
6. Salvar a terceira visualização em um tópico no Kafka
7. Criar a visualização pelo Spark com os dados enviados para o HDFS:
    - Síntese de casos, óbitos, incidência e mortalidade
8. Salvar a visualização do exercício 6 em um tópico no Elastic
9. Criar um dashboard no Elastic para visualização dos novos dados enviados. 

