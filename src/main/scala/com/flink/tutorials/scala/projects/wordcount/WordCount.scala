package com.flink.tutorials.scala.projects.wordcount

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

object WordCount {

  def main(args: Array[String]): Unit = {

    // Scala Execution Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-group")
    val inputTopic: String = "Shakespeare"
    val outputTopic: String = "WordCount"

    // Source
    val consumer: FlinkKafkaConsumer[String] =
      new FlinkKafkaConsumer[String](inputTopic, new SimpleStringSchema(), properties)
    val stream: DataStream[String] = env.addSource(consumer)

    // Transformations
    val wordCount: DataStream[(String, Int)] = stream
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    // Sink
    val producer: FlinkKafkaProducer[(String, Int)] =
      new FlinkKafkaProducer[(String, Int)](
        outputTopic,
        new KafkaWordCountSerializationSchema(outputTopic),
        properties,
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    wordCount.addSink(producer)

    // execute
    env.execute("kafka streaming word count")

  }

  class KafkaWordCountSerializationSchema(topic: String) extends KafkaSerializationSchema[(String, Int)] {

    // 序列化
    override def serialize(element: (String, Int), aLong: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      new ProducerRecord[Array[Byte], Array[Byte]](topic, (element._1 + ": " + element._2).getBytes(StandardCharsets.UTF_8))
    }

  }

}
