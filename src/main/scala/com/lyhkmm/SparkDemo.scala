package com.lyhkmm

/**
  *
  * ＠author linyuanhuang@lvmama.com
  * 2019/3/1 10:42
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object SparkDemo {
  def main(args: Array[String]) {
    //待处理的文件
    // 创建目录 hadoop fs -mkdir -p spark_demo/input/ hadoop fs -mkdir -p spark_demo/input/
    // 上传linux本地文件到hdfs
    val testFile = "hdfs://hadoop1:9000/spark_demo/input/words.txt"
    val conf = new SparkConf().setAppName("SparkDemo Application")
      //.setMaster("local")
      //远程调试spark standalone集群
      .setJars(List("E:\\OneDrive\\study\\spark\\out\\artifacts\\spark_jar\\spark.jar")).setMaster("spark://hadoop1:7077")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(testFile)
    //处理之后文件路径
    val wordcount = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))
    wordcount.saveAsTextFile( "hdfs://hadoop1:9000/spark_demo/out/"+System.currentTimeMillis())
    sc.stop()
  }
}
