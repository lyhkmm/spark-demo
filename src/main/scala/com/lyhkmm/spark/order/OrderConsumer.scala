package com.lyhkmm.spark.order

import com.alibaba.fastjson.JSON
import com.lyhkmm.spark.utils.RedisClient
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object OrderConsumer {
  //Redis配置
  val dbIndex = 0
  //每件商品总销售额
  val orderTotalKey = "app::order::total"
  //每件商品每分钟销售额
  val oneMinTotalKey = "app::order::product"
  //总销售额
  val totalKey = "app::order::all"

  def main(args: Array[String]): Unit = {

    // 创建 StreamingContext 时间片为1秒
    val conf = new SparkConf().setMaster("local").setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Kafka 配置
    val topics = Set("order")
    val brokers = "127.0.0.1:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")
    // 创建一个 direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //解析JSON
    val events = kafkaStream.flatMap(line => Some(JSON.parseObject(line._2)))
    // 按ID分组统计个数与价格总合
    val orders = events.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))

    //输出
    orders.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {
          println("id=" + x._1 + " count=" + x._2 + " price=" + x._3)
          //保存到Redis中
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          //每个商品销售额累加
          jedis.hincrBy(orderTotalKey, x._1, x._3)
          //上一分钟第每个商品销售额
          jedis.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          jedis.incrBy(totalKey, x._3)
          RedisClient.pool.returnResource(jedis)
        })
      ))
    ssc.start()
    ssc.awaitTermination()
  }
}
