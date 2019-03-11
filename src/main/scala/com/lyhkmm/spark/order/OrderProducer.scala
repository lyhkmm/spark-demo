package com.lyhkmm.spark.order

import java.util.Properties
import com.alibaba.fastjson.JSONObject
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.util.Random

object OrderProducer {

  def main(args: Array[String]): Unit = {
    //Kafka参数设置
    val topic = "order"
    val brokers = "127.0.0.1:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val kafkaConfig = new ProducerConfig(props)
    //创建生产者
    val producer = new Producer[String, String](kafkaConfig)
    while (true) {
      //随机生成10以内ID
      val id = Random.nextInt(10)
      //创建订单事件
      val event = new JSONObject();
      event.put("id", id)
      event.put("price", Random.nextInt(10000))

      //发送信息
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)
      //随机暂停一段时间
      Thread.sleep(Random.nextInt(100))
    }
  }
}
