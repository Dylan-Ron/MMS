package igniter

import java.text.{ParseException, SimpleDateFormat}

import entity.{CarPoint, MatchedPoint}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import rtree.RTree
import mapmatching.Entrance.matchingEntranceForSpark
import calculator.Calculator.zoneToLocalTime
import org.apache.htrace.fasterxml.jackson.databind.deser.std.StringDeserializer

import scala.Array._

object Matching {

  def main(args: Array[String]): Unit =
  {
    // 基本假设:GPS轨迹点以时间顺序流出

    if(args.length != 7)
    {
      System.err.println("Parameters:sparkMaster,checkpoint,roadNet,range,host,port,outputPath")
      //System.err.println("Parameters:<sparkMaster> <checkpoint_dir> <roadNet> <range> <zkQuorum> & ")
      //System.err.println("<topic-group> <topics> <partition_num> <OutputPath>")
      System.exit(1)
    }

    val Array(sparkMaster,checkpoint,roadNet,range,host,port,outputPath) = args

    val conf = new SparkConf().setAppName("MM on Stream").setMaster(sparkMaster).set("spark.driver.allowMultipleContexts","true")

    // 生成原生树并广播
    val sc = new SparkContext(conf)

    val roads = sc.textFile(roadNet).collect() // 读取路网文件并形成真实RDD

    val tree = rtree.GetSparkTree.getTree(roads) //生成原生树

    //val broadcast = sc.broadcast(tree) // 广播原生树

    /////创建泉眼
    val ssc = new StreamingContext(conf,Seconds(2)) // 创建水闸,窗口期为2s

    ssc.checkpoint(checkpoint)  // 创建检查点，不然updateStateByKey的历史状态存储在哪呢？

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> 1
    )

    val pointStream = ssc.socketTextStream(host,port.toInt) //pointStream是泉眼,指定了监听的host和端口

    ///////////////////对pointStream的操作，持续自执行流程序段，时间间隔为2s start////////////////////////////////

    val step1 = pointStream.map(s => s.split(",")) // [String] => [Array[String]] 长串转小串数组
/*
    step1.foreachRDD(rdd => // Dstream中的每一个RDD
      rdd.foreachPartition { partition =>  // RDD中的每一个partition
        partition.foreach { item => // partition中的每一个元素
          item.foreach(println)
        }
      }
    )
 */

    val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") //Date转换器,用于String -> Date

    //val ft_bro = sc.broadcast(ft) // 广播转换器

    val step2 = step1.map(
        fields =>
          (fields(0),new CarPoint(fields(0),fields(1).trim.toDouble,fields(2).trim.toDouble,ft.parse(fields(3)),fields(4).trim.toDouble))
      )   // [Array[String]] => <id,CarPoint>

      val step3 = step2.groupByKey().mapValues(f => f.toArray) // <id,CarPoint> => <id,Array[CarPoint]>

    step3.foreachRDD(rdd => // Dstream中的每一个RDD
      rdd.foreachPartition { partition =>  // RDD中的每一个partition
        partition.foreach { item => // partition中的每一个元素
          //val rowkey = item._1
          for(point <- item._2 )
          {
            print(point.toString)
            //val longitude = point.getLongitude
            //val latitude = point.getLatitude
            //val time = point.getTimestamp
            //val value = longitude+","+latitude+","+time.toString
            //putRowData(table,rowkey,"cf","nb",value)
            //putRowData(table,rowkey,"cf","nb",value)
          }
        }
      }
    )

    /*

      // CarPoint对象比较器
      def sortRule(carPoint: CarPoint ): Long = carPoint.timestamp.getTime() // 返回自从GMT 1970-01-01 00:00:00到此date对象的"距离",单位:ms

      val step4 = step3.mapValues(f => f.sortBy(sortRule))  //  <id,(SortedCarPoints)>

      /*
      关于updateStateByKey这个函数，它默认应用于DStream中包含多个Key值相同的<K,V>的场景
      例如wordCount，DStream中可能包含多个<hello,1>
      但在这里,由于之前执行了groupByKey,所以不存在Key值相同的 <车牌号,Array[CarPoint]>
      于是,对于updateFunc的第一个参数:一个Seq,指代所有Key相同的<K,V>的values聚合成的Seq
      这个Seq中必然只有1个Array[CarPoint],因为不存在Key值相同的 <车牌号,Array[CarPoint]>嘛
       */

      //println(broadcast.value.getTreeLevel)

      val updateFunc = (values:Seq[Array[CarPoint]],state:Option[Array[MatchedPoint]]) =>
      {
        val current = matchingEntranceForSpark(values.apply(0),range.toInt,tree) //对此时的点进行匹配

        val previous = state.getOrElse(Array[MatchedPoint]()) // 取历史状态,

        // 返回更新后的state (Option,Some这个点有待搞懂)
        Some(concat(previous,current)) //合并它俩，previous在前,current在后
      }

      val step5 = step4.updateStateByKey[Array[MatchedPoint]](updateFunc)  // 执行有状态计算

      //createOneCFTable(admin,"test","cf")

      // 将DStream中的匹配结果写到本地上来
      step5.foreachRDD(rdd => // Dstream中的每一个RDD
        rdd.foreachPartition { partition =>  // RDD中的每一个partition
          partition.foreach { item => // partition中的每一个元素
            //val rowkey = item._1
            for(point <- item._2 )
            {
              print(point.toString)
              //val longitude = point.getLongitude
              //val latitude = point.getLatitude
              //val time = point.getTimestamp
              //val value = longitude+","+latitude+","+time.toString
              //putRowData(table,rowkey,"cf","nb",value)
              //putRowData(table,rowkey,"cf","nb",value)
            }
          }
        }
      )

      //step5.print() // 输出结果,还不知道效果怎么样，如果不行那就后面再换

     */

    ///////////////////////////自循环流程序段 end//////////////////////////////////////////////////////////////////

    ssc.start() // 水闸通电

    ssc.awaitTermination()  // 等待水闸断电
  }

  /**
   * 版本2 仅仅保留核心的rdd转换流程,将其他工作分担给了主函数:广播2D-RTree,获取taxi_rdd1.比版本1更灵活，因为解放了taxi_rdd1的获取方式
   * @param taxi_rdd1 这个RDD中的元素需要预处理成一个个String:"id,longitude,latitude,time(yyyy-MM-dd hh:mm:ss),rate"
   * @param broadcast 2D-RTree的广播变量
   * @param range 抓路范围
   * @return
   */
  def matchingOnSpark(taxi_rdd1:RDD[String], broadcast: Broadcast[RTree], range:Double):RDD[(String,Array[MatchedPoint])] =
  {
    val taxi_rdd2 = taxi_rdd1.map(_.split(",")) // rdd元素:Array[String],将一行按逗号拆分出的字符串数组

    val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") //Date转换器

    val taxi_rdd3 = taxi_rdd2.map(
      fields => (fields(0),new CarPoint(fields(0),fields(1).trim.toDouble,fields(2).trim.toDouble,ft.parse(fields(3)),fields(4).trim.toDouble))
    ) // rdd元素:<车牌号,CarPoint(包含5个属性)> 其实在这个版本的Map-Matching中不会考虑车速,所以fields(4)其实就没用到

    val taxi_rdd4 = taxi_rdd3.groupByKey() // rdd元素:<车牌号,Iterable<无序的CarPoint>>

    // CarPoint对象比较器
    def sortRule(carPoint: CarPoint ): Long = carPoint.timestamp.getTime() // 返回自从GMT 1970-01-01 00:00:00到此date对象的"距离",单位:ms

    val taxi_rdd5 = taxi_rdd4.mapValues(f => f.toArray.sortBy(sortRule)) // rdd元素:<车牌号,Array<有序的CarPoint>>

    val tree_regenerate = broadcast.value // 获取广播之后的再生树

    taxi_rdd5.mapValues(f => matchingEntranceForSpark(f,range,tree_regenerate)) //匹配结果:一个RDD,元素是<车牌号,匹配后的车辆轨迹>
  }

}

