import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {




  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))

    val kafka_brokers: String = ConfigurationManager.config.getString(Constants.kAFKA_BROKERS)

    val kafka_topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers"->kafka_brokers,
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))

    //adReadTimeValueDStream: Dstream[RDD,RDD,RDD]  RDD[String]
    val adReadTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream = adReadTimeValueDStream.transform{
      logRDD=>
        //blackListArray: Array[AdBlackList]  AdBlacklist: userId
        val blackListArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()

        //userIdArray: Array[Long]  [userId1,userId2,userId3...]
         val userIdArray: Array[Long] = blackListArray.map(item => item.userid)

        logRDD.filter{
          case log =>
            //log: timestamp  province,  city  userid,   adid
            val logSplit: Array[String] = log.split(" ")
            val userId: Long = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }

    streamingContext.checkpoint("./spark-streaming")

    adReadTimeValueDStream.checkpoint(Duration(10000))

    //需求7 实时维护黑名单
    gengerateBlackList(adRealTimeFilterDStream)


    //需求8 各省各城市广告点击量实时统计（累积统计）
    provinceCityClickStat(adRealTimeFilterDStream)

    adRealTimeFilterDStream.foreachRDD(rdd=>rdd.foreach(println(_)))
    streamingContext.start()
    streamingContext.awaitTermination()
  }


  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {

    //key2ProvinceCityDStream:DStream[RDD[key,1L]]
    var key2ProvinceCityDStream  = adRealTimeFilterDStream.map {
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        //dateKey: yy-mm-dd
        val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))
        val province: String = logSplit(1)
        val city: String = logSplit(2)
        val adid: String = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid

        (key, 1L)
    }

    //updateStateByKey 会不停的进行checkpoint与反序列化操作
    //即所有的DStream中的rdd都会被updateStateBykey操作，进行全局的累积操作
    val key2StateDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get

        for (value <- values) {
          newValue += value
        }
        Some(newValue)
    }


    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()
          for ((key,count) <- items){
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val province: String = keySplit(1)
            val city: String = keySplit(2)
            val adid: Long = keySplit(3).toLong

            adStatArray += AdStat(date,province,city,adid,count)
          }

          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }



  }


  def gengerateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: String = logSplit(0).toString
        val dateKey: String = DateUtils.formatDate(new Date(timeStamp))
        val userId: Long = logSplit(3).toLong
        val adid: Long = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adid
        (key, 1L)

    }
    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_+_)

    //执行数据库的updateBatch操作
    //根据每一个RDD里面的数据，更新用户点击次数表
    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
       //todo 注意这里的items是每一个partion中的Dsteam，因此一个items会包含多个key-value（String-long）类型的rdd
       //todo 因此在下面需要遍历每一个item，遍历时加入到AduserClickCount对象中，再将该对象加入到arrayBuffer里面
       items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for ((key,count) <- items){
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val userId: Long = keySplit(1).toLong
            val adId: Long = keySplit(2).toLong

            clickCountArray += AdUserClickCount(date,userId,adId,count)
          }

          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }

    //调用findClickCountByMultkey方法 即从数据库中重新查出 key value的值,并进行判断
    val key2BlackListDStream: DStream[(String, Long)] = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date: String = keySplit(0)
        val userId: Long = keySplit(1).toLong
        val adid: Long = keySplit(2).toLong

        val clickCount: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        if (clickCount > 100) {
          true
        } else {
          false
        }
    }

    //todo 接下来要执行黑名单入库操作
    //TODO key2BlackListDStream.map: DStream[RDD[userId]]
    val userIdDStream: DStream[Long] = key2BlackListDStream.map {
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        item =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for (userId <- item){
            userIdArray+=AdBlacklist(userId)
          }

          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }


  }


}
