package sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingKafka {

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkStreamingKafka",
      "auto.offset.reset" -> "latest"
    )

    val topics = Array("java")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafkaApp")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkStreamingKafkaApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val result = kafkaStream.map(record => (record.key(), record.value()))

    result.foreachRDD(x => {
      val tmp = x.map(x => x._2)
      tmp.foreach(x => println(x))
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
