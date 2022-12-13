from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime, date
import org.elasticsearch.spark.sql
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object ProjectCovid19 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("ProjectCovid19").getOrCreate()

    val covid = spark.read.csv("hdfs://namenode:8020/user/gabriel/data/H*", header = "true", sep = ";", inferSchema = True)

    // Remoção de valores nulos da coluna 'Recuperadosnovos','emAcompanhamentoNovos',' populacaoTCU2019'

    val covid2 = covid.na.fill(value = '0', subset =['Recuperadosnovos', 'emAcompanhamentoNovos',' populacaoTCU2019'] )

    val cvbr2 = covid2.withColumnRenamed('Recuperadosnovos',' CasosRecuperados '
      .withColumnRenamed('emAcompanhamentoNovos',' EmAcompanhamento ')
      .withColumnRenamed('casosAcumulado','Acumulado')
      .withColumnRenamed('data',' Atualizacao')

    val cvbr3 = cvbr2.withColumn('Obitos', col('Acumulado') + col('obitosNovos'))

    val cvbr4 = cvbr3.withColumn('Casos', col('Acumulado') + col('casosNovos'))

    // Criando a coluna de coeficiente de incidência.

    val cvbr5 = cvbr4.withColumn('Incidencia_por_100mil_hab', col('Obitos') *  100 / col('Casos') )

    //Criando a coluna da taxa de mortalidade

    val cvbr6 = cvbr5.withColumn('Mortalidade_por_100mil_hab', col('Obitos') /100000)

    //Criando a coluna da taxa de letalidade.//Criando a coluna da taxa de letalidade.

    val cvbr7 = cvbr6.withColumn('Letalidade', col('Obitos') / col 'Casos') )

    // Limpando do dataframe as colunas que não servirão para as views
    val cvbr7_cleaned = cvbr7.drop('interior' / 'metropolitana','codRegiaoSaude','codmun')

    // Salvando o dataframe no hive como tabela particionada por municipio
    val cvbr8 = cvbr7_cleaned.write.partitionBy('municipio').saveAsTable("covidbr")

    // PRIMEIRA VIEW : CASOS NÃO CONFIRMADOS

    // A soma de todos os casos recuperados e a soma dos casos em acompanhamento.


    val cvbr7_cleaned.createOrReplaceTempView("casos")

    val cs = spark.sql("SELECT SUM(CasosRecuperados) as Casos_Recuperados, \
      SUM(EmAcompanhamento) as Em_Observacao FROM "casos")

    //Salvando como tabela Hive
    val cs.write.mode("overwrite").saveAsTable("CASOS")

    // SEGUNDA VIEW : CASOS CONFIRMADOS

    // Coeficiente de Incidência por 100mil/hab:(número de óbitos x 100.000)/número de casos confirmados
    //
    //Número de óbitos: soma dos óbitos novos e acumulados
    //
    //Número de casos: soma dos casos acumulados com os novos

    val cvbr7_cleaned.createOrReplaceTempView("casos_confirmados")
    val cs_confirm = spark.sql("SELECT SUM(Incidencia_por_100mil_hab) as Incidencia, \
      SUM(Casos) as Casos, \
      SUM (Acumulado) as Acumulado FROM casos_confirmados")
      cs_confirm.show()

    // Salvando a segunda view em formato parquet e com compressão snappy
    val cs_confirm.write.mode("overwrite").option("compression","snappy").saveAsTable("CASOS")

    // Terceira View: Óbitos

    // Óbitos Acumulados
    //Novos Óbitos
    //Taxa de letalidade: total de mortes \ total de casos ocorridos até agora
    //Taxa de mortalidade: total de mortes \ população (100000)
    //


    val cvbr7_cleaned.createOrReplaceTempView("obitos")
    val ob = spark.sql("SELECT SUM(obitosNovos) as Novos_Obitos, SUM (obitosAcumulado) as Obitos_Acumula dos,SUM (Letalidade) as Letalidade,SUM (Mortalidade_por_100mil_hab) as Mortalidade_por_100_hab FROM obitos" )
    ob.show()

    // Salvar em topico kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false)
    )

    val ob.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers")
      .option("topic1")
      .save()

    // Quarta View: Dados enviados para o HDFS
    //
    // Casos | Óbitos | Indicencia 100k/hab | Mortalidade | Atualização
    // Incidência por 100K habitantes Método de cálculo: (casos confirmados * 100.000) / população
    // Mortalidade -  Método de cálculo: (óbitos * 100.000) / população  -> por 100K habitantes

    val casos = casos_df.groupBy('regiao', 'municipio').agg(sum('casos_novos').alias('casos'),
        sum('obitos_novos').alias('obitos'),
        last('data').alias('atualizacao'),
        last('populacao').alias('agg_pop'),
    ) \
    .withColumn('incidencia', round((col('casos') * 100_000) / col('agg_pop'), 1)) \
    .withColumn('mortalidade', round((col('obitos') * 100_000) / col('agg_pop'), 1)) \
    .select('regiao', 'municipio', 'casos', 'obitos', 'incidencia', 'mortalidade', 'atualizacao') \
    .orderBy(col('municipio').asc_nulls_first(), 'regiao')

    val casos.show()

      // Salvando a terceira visualização em um tópico Elastic

    ES_NODES=''
    ES_PORT=''
    ES_NET_HTTP_AUTH_USER=''
    ES_NET_HTTP_AUTH_PASS=''
    ES_NET_SLL='true'
    ES_NODE_WAN_ONLY='true'
    ES_WRITE_OPERATION = 'upsert'

    def get_elastic_config_options() -> dict:
      return {
          'es.nodes': ES_NODES,
          'es.port': ES_PORT,
          'es.net.http.auth.user': ES_NET_HTTP_AUTH_USER,
         'es.net.http.auth.pass': ES_NET_HTTP_AUTH_PASS,
         'es.net.sll': ES_NET_SLL,
          'es.nodes.wan.only': ES_NODE_WAN_ONLY,
         'es.write.operation': ES_WRITE_OPERATION
      }
    ELASTIC_OPTIONS = get_elastic_config_options()

    val cs.write \
      .format('org.elasticsearch.spark.sql') \
      .options(**ELASTIC_OPTIONS)
      .mode('append') \
      .save('casos')

   val cs_confirm.write \
      .format('org.elasticsearch.spark.sql') \
      .options(**ELASTIC_OPTIONS)
      .mode('append') \
      .save('casos_confirmados')

    val ob.write \
      .format('org.elasticsearch.spark.sql') \
      .options(**ELASTIC_OPTIONS)
      .mode('append') \
      .save('casos_confirmados')






  }
}
