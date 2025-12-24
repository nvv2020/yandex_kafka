```bash
# Загружаем коннектор в Connect
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-hdfs/versions/10.6.4/confluentinc-kafka-connect-hdfs-10.6.4.zip
unzip confluentinc-kafka-connect-hdfs-10.6.4.zip -d /path/to/connect/plugins/

# Создаем файл hdfs-sink-config.json:
json
{
  "name": "hdfs-sink-primary-topics",
  "config": {
    "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
    "tasks.max": "2",
    "topics": "primary.shop-clear,primary.client-searches",
    "hdfs.url": "hdfs://hadoop-namenode:9000",
    "topics.dir": "/opt/nfs/share/data/kafka/topics",
    "logs.dir": "/logs",
    "format.class": "io.confluent.connect.hdfs.parquet.ParquetFormat",
    "partitioner.class": "io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
    "locale": "en-US",
    "timezone": "UTC",
    "flush.size": "1000",
    "rotate.interval.ms": "300000",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    "schema.compatibility": "BACKWARD",
    "hadoop.conf.dir": "/opt/hadoop/etc",
    "hadoop.home": "/opt/hadoop"
  }
}

# /opt/hadoop/etc/hadoop

Шаг 3.3: Запуск коннектора
bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @hdfs-sink-config.json \
  http://localhost:8083/connectors
4. Spark Job для рекомендаций
SparkRecommender.scala:
scala
package com.example.recommender

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object SparkRecommender {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ProductRecommender")
      .getOrCreate()
    
    import spark.implicits._

    // 1. Читаем данные товаров из HDFS
    val productsDF = spark.read
      .parquet("hdfs://hadoop-namenode:9000/data/kafka/topics/primary.shop-clear/*/*/*/*.parquet")
    
    // 2. Читаем поисковые запросы из HDFS
    val searchesDF = spark.read
      .parquet("hdfs://hadoop-namenode:9000/data/kafka/topics/primary.client-searches/*/*/*/*.parquet")
    
    // 3. Преобразуем структуры данных
    val productsExploded = productsDF
      .select(
        $"product_id",
        $"name".as("product_name"),
        $"description",
        explode($"tags").as("tag"),
        $"category",
        $"brand"
      )
    
    // 4. Взрываем поисковые слова
    val searchesExploded = searchesDF
      .select(
        $"client_id",
        explode($"search_words").as("search_word")
      )
    
    // 5. Находим совпадения (простой алгоритм)
    val recommendations = searchesExploded
      .join(productsExploded, 
        lower(productsExploded("tag")).contains(lower(searchesExploded("search_word"))) ||
        lower(productsExploded("description")).contains(lower(searchesExploded("search_word")))
      )
      .select(
        $"client_id",
        $"product_name",
        $"search_word",
        $"tag",
        lit(1.0).as("relevance_score")
      )
      .groupBy($"client_id", $"product_name")
      .agg(
        count($"search_word").as("match_count"),
        collect_list($"search_word").as("matched_words")
      )
      .filter($"match_count" >= 1)
      .orderBy($"client_id", $"match_count".desc)
      .limit(10) // Топ-10 рекомендаций на клиента
    
    // 6. Публикуем в Kafka
    recommendations
      .select(
        to_json(struct(
          $"client_id",
          $"product_name",
          $"match_count",
          $"matched_words"
        )).as("value")
      )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-broker:9092")
      .option("topic", "client-recommendations")
      .save()
    
    spark.stop()
  }
}
build.sbt:
scala
name := "spark-recommender"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
)
Сборка и запуск:
bash
# Сборка JAR
sbt package

# Копируем JAR в контейнер
docker cp target/scala-2.12/spark-recommender_2.12-1.0.jar spark-worker-1:/jobs/

# Запуск через spark-submit (обновляем команду в docker-compose):
spark-submit \
  --master spark://spark-master:7077 \
  --class com.example.recommender.SparkRecommender \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.executor.memory=2G \
  --conf spark.driver.memory=1G \
  /jobs/spark-recommender_2.12-1.0.jar
Проверка результатов:
bash
# Читаем рекомендации из Kafka
kafkacat -b localhost:9092 -t client-recommendations -C -e


  ```