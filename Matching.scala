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
    // ��������:GPS�켣����ʱ��˳������

    if(args.length != 7)
    {
      System.err.println("Parameters:sparkMaster,checkpoint,roadNet,range,host,port,outputPath")
      //System.err.println("Parameters:<sparkMaster> <checkpoint_dir> <roadNet> <range> <zkQuorum> & ")
      //System.err.println("<topic-group> <topics> <partition_num> <OutputPath>")
      System.exit(1)
    }

    val Array(sparkMaster,checkpoint,roadNet,range,host,port,outputPath) = args

    val conf = new SparkConf().setAppName("MM on Stream").setMaster(sparkMaster).set("spark.driver.allowMultipleContexts","true")

    // ����ԭ�������㲥
    val sc = new SparkContext(conf)

    val roads = sc.textFile(roadNet).collect() // ��ȡ·���ļ����γ���ʵRDD

    val tree = rtree.GetSparkTree.getTree(roads) //����ԭ����

    //val broadcast = sc.broadcast(tree) // �㲥ԭ����

    /////����Ȫ��
    val ssc = new StreamingContext(conf,Seconds(2)) // ����ˮբ,������Ϊ2s

    ssc.checkpoint(checkpoint)  // �������㣬��ȻupdateStateByKey����ʷ״̬�洢�����أ�

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> 1
    )

    val pointStream = ssc.socketTextStream(host,port.toInt) //pointStream��Ȫ��,ָ���˼�����host�Ͷ˿�

    ///////////////////��pointStream�Ĳ�����������ִ��������Σ�ʱ����Ϊ2s start////////////////////////////////

    val step1 = pointStream.map(s => s.split(",")) // [String] => [Array[String]] ����תС������
/*
    step1.foreachRDD(rdd => // Dstream�е�ÿһ��RDD
      rdd.foreachPartition { partition =>  // RDD�е�ÿһ��partition
        partition.foreach { item => // partition�е�ÿһ��Ԫ��
          item.foreach(println)
        }
      }
    )
 */

    val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") //Dateת����,����String -> Date

    //val ft_bro = sc.broadcast(ft) // �㲥ת����

    val step2 = step1.map(
        fields =>
          (fields(0),new CarPoint(fields(0),fields(1).trim.toDouble,fields(2).trim.toDouble,ft.parse(fields(3)),fields(4).trim.toDouble))
      )   // [Array[String]] => <id,CarPoint>

      val step3 = step2.groupByKey().mapValues(f => f.toArray) // <id,CarPoint> => <id,Array[CarPoint]>

    step3.foreachRDD(rdd => // Dstream�е�ÿһ��RDD
      rdd.foreachPartition { partition =>  // RDD�е�ÿһ��partition
        partition.foreach { item => // partition�е�ÿһ��Ԫ��
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

      // CarPoint����Ƚ���
      def sortRule(carPoint: CarPoint ): Long = carPoint.timestamp.getTime() // �����Դ�GMT 1970-01-01 00:00:00����date�����"����",��λ:ms

      val step4 = step3.mapValues(f => f.sortBy(sortRule))  //  <id,(SortedCarPoints)>

      /*
      ����updateStateByKey�����������Ĭ��Ӧ����DStream�а������Keyֵ��ͬ��<K,V>�ĳ���
      ����wordCount��DStream�п��ܰ������<hello,1>
      ��������,����֮ǰִ����groupByKey,���Բ�����Keyֵ��ͬ�� <���ƺ�,Array[CarPoint]>
      ����,����updateFunc�ĵ�һ������:һ��Seq,ָ������Key��ͬ��<K,V>��values�ۺϳɵ�Seq
      ���Seq�б�Ȼֻ��1��Array[CarPoint],��Ϊ������Keyֵ��ͬ�� <���ƺ�,Array[CarPoint]>��
       */

      //println(broadcast.value.getTreeLevel)

      val updateFunc = (values:Seq[Array[CarPoint]],state:Option[Array[MatchedPoint]]) =>
      {
        val current = matchingEntranceForSpark(values.apply(0),range.toInt,tree) //�Դ�ʱ�ĵ����ƥ��

        val previous = state.getOrElse(Array[MatchedPoint]()) // ȡ��ʷ״̬,

        // ���ظ��º��state (Option,Some������д��㶮)
        Some(concat(previous,current)) //�ϲ�������previous��ǰ,current�ں�
      }

      val step5 = step4.updateStateByKey[Array[MatchedPoint]](updateFunc)  // ִ����״̬����

      //createOneCFTable(admin,"test","cf")

      // ��DStream�е�ƥ����д����������
      step5.foreachRDD(rdd => // Dstream�е�ÿһ��RDD
        rdd.foreachPartition { partition =>  // RDD�е�ÿһ��partition
          partition.foreach { item => // partition�е�ÿһ��Ԫ��
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

      //step5.print() // ������,����֪��Ч����ô������������Ǿͺ����ٻ�

     */

    ///////////////////////////��ѭ��������� end//////////////////////////////////////////////////////////////////

    ssc.start() // ˮբͨ��

    ssc.awaitTermination()  // �ȴ�ˮբ�ϵ�
  }

  /**
   * �汾2 �����������ĵ�rddת������,�����������ֵ�����������:�㲥2D-RTree,��ȡtaxi_rdd1.�Ȱ汾1������Ϊ�����taxi_rdd1�Ļ�ȡ��ʽ
   * @param taxi_rdd1 ���RDD�е�Ԫ����ҪԤ�����һ����String:"id,longitude,latitude,time(yyyy-MM-dd hh:mm:ss),rate"
   * @param broadcast 2D-RTree�Ĺ㲥����
   * @param range ץ·��Χ
   * @return
   */
  def matchingOnSpark(taxi_rdd1:RDD[String], broadcast: Broadcast[RTree], range:Double):RDD[(String,Array[MatchedPoint])] =
  {
    val taxi_rdd2 = taxi_rdd1.map(_.split(",")) // rddԪ��:Array[String],��һ�а����Ų�ֳ����ַ�������

    val ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") //Dateת����

    val taxi_rdd3 = taxi_rdd2.map(
      fields => (fields(0),new CarPoint(fields(0),fields(1).trim.toDouble,fields(2).trim.toDouble,ft.parse(fields(3)),fields(4).trim.toDouble))
    ) // rddԪ��:<���ƺ�,CarPoint(����5������)> ��ʵ������汾��Map-Matching�в��ῼ�ǳ���,����fields(4)��ʵ��û�õ�

    val taxi_rdd4 = taxi_rdd3.groupByKey() // rddԪ��:<���ƺ�,Iterable<�����CarPoint>>

    // CarPoint����Ƚ���
    def sortRule(carPoint: CarPoint ): Long = carPoint.timestamp.getTime() // �����Դ�GMT 1970-01-01 00:00:00����date�����"����",��λ:ms

    val taxi_rdd5 = taxi_rdd4.mapValues(f => f.toArray.sortBy(sortRule)) // rddԪ��:<���ƺ�,Array<�����CarPoint>>

    val tree_regenerate = broadcast.value // ��ȡ�㲥֮���������

    taxi_rdd5.mapValues(f => matchingEntranceForSpark(f,range,tree_regenerate)) //ƥ����:һ��RDD,Ԫ����<���ƺ�,ƥ���ĳ����켣>
  }

}

