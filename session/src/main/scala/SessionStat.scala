import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionStat {




  def main(args: Array[String]): Unit = {

    //1 获取筛选条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    //2 将json串转成jsonObject
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    //3 创建全局唯一的主键
    val taskUUID: String = UUID.randomUUID().toString

    //4 创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    //5 创建sparksession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    //6 获取原始的动作表数据
    var actionRDD = getOriactionRDD(sparkSession,taskParam)

    //7 聚合操作
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id,item))

    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    session2GroupActionRDD.cache()

    //todo 7.1根据每一组聚合数据提取成一条聚合信息
//    session2GroupActionRDD.foreach(println(_))
    //TODO 7.2 暂时获取动作表的聚合数据
//    val userIdAggreInfoRDD: RDD[(Long, String)] = getSessionFullInfo(sparkSession,session2GroupActionRDD)

    //todo 7.3 联立用户表 获取动作表&用户表完整数据 (即从一个斧子型数据到一条聚合信息)
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession,session2GroupActionRDD)

    //8 创建自定义累加器
    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    //9 过滤数据
    //sessionId2FilterRDD : RDD[(sessionId,fullInfo)]
    val sessionId2FilterRDD = getSessionFilterRDD(taskParam,sessionId2FullInfoRDD,sessionAccumulator)

    //收集数据到数据库前要加上action算子
//    sessionId2FilterRDD.foreach(println(_))

    //10 从累加器中获取结果
    getSessionRatio(sparkSession,taskUUID,sessionAccumulator.value)

    //需求二 session随机抽取
    //sessionId2FilterRDD ： RDD[(sid,fullInfo)] 一个session对应一条数据，也就是fullinfo
    sessionRandomExtract(sparkSession,taskUUID,sessionId2FilterRDD)

    //求符合过滤条件的actionRDD数据
    //sessionId2ActionRDD : RDD[(sessionId，action)]
    //sessionId2FilterRDD : RDD[(sessionId,fullInfo)]
    val sessionId2FilterActionRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    //3 需求三 top10 品类
    //top10CategoryArray :Array[Long]
    var top10CategoryArray = top10PopularCategories(sparkSession,taskUUID,sessionId2FilterActionRDD)


    //4 需求四
    //sessionId2FilterActionRDD : RDD[(sessionId,action)]
    top10ActiveSession(sparkSession,taskUUID,sessionId2FilterActionRDD,top10CategoryArray)
  }


  def top10ActiveSession(sparkSession: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], top10CategoryArray: Array[(SortKey, String)]): Unit = {
   //cidArray: Array[Long] 包含了top10热门品类id
    val cidArray: Array[Long] = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    //到这就获得了所有符合过滤条件的 并且点击过top10热门品类的action数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }

    //按照sessionid进行聚合操作 获得斧子型数据
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    //然后遍历上面这个斧子型数据的value(一个可迭代的itable类型的数据，并通过map来维护category的个数)
   //cid2SessionCountRDD :RDD[(cid,sessionCount)]
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap{
      case (sessionId,iterableAction) =>
        val categoryCountMap: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()

        for (action <- iterableAction){
          val cid: Long = action.click_category_id
          if (!categoryCountMap.contains(cid)){
            categoryCountMap += (cid ->1)
          }
          categoryCountMap.update(cid,categoryCountMap(cid)+1)
        }
        //记录了一个sessionId对于他所有的点击过的品类的点击次数
//        categoryCountMap
        //yield起到一个采集的作用
        for ((cid,count) <- categoryCountMap)
          yield (cid,sessionId+ "=" + count)
    }

    //cid2GroupRDD: RDD[(cid,iterableSessionCount)]
    //对cid2SessionCountRDD执行groupbykey 就可以获得一个cid的所有session的点击次数
    val cid2GroupRDD: RDD[(Long, Iterable[String])] = cid2SessionCountRDD.groupByKey()

    //top10SesionRDD :RDD[Top10Session]
    val top10SesionRDD: RDD[Top10Session] = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        val sortList: List[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)

        var top10Session = sortList.map {
          case item =>
            val sessionId: String = item.split("=")(0)
            val sessionCount: Long = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, sessionCount)
        }
        top10Session
    }
    import sparkSession.implicits._
    top10SesionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }


  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //第一步：是要获取所有发生过点击，下单，付款的品类
    var cid2CidRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (sid, action) =>
        var categoryBuffer = new ArrayBuffer[(Long, Long)]()
        //点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(",")) {
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(",")) {
            categoryBuffer += ((payCid.toLong, payCid.toLong))
          }
        }
        categoryBuffer
    }
    //执行去重操作
    cid2CidRDD = cid2CidRDD.distinct()

    //第二步：统计品类的点击次数
    var cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
//    cid2ClickCountRDD.foreach(println(_))

    //第三步 统计品类的付款次数
    var pay2clickCountRDD = getPayCount(sessionId2FilterActionRDD)
//    pay2clickCountRDD.foreach(println(_))

    //第四步 统计品类的下单次数
    var order2clickCountRDD = getOrderCount(sessionId2FilterActionRDD)
//    order2clickCountRDD.foreach(println(_))

    //第五步，整合点击次数，付款次数，下单次数
    //数据格式 ：(92,categoryid=92|clickCount=75|orderCount=82|payCount=80)
    //cid2FullInfoRDD :RDD[(cid,countInfo)]
    var cid2FullInfoRDD = getFullCount(cid2CidRDD,cid2ClickCountRDD,order2clickCountRDD,pay2clickCountRDD)
//    cid2FullInfoRDD.foreach(println(_))

    //实现自定义二次排序key
    val sortKey2FullCountRDD = cid2FullInfoRDD.map {
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
    }

    val top10SortKey2FullCountArray: Array[(SortKey, String)] = sortKey2FullCountRDD.sortByKey(false).take(10)

    val top10CategoryRDD: RDD[Top10Category] = sparkSession.sparkContext.makeRDD(top10SortKey2FullCountArray).map {
      case (sortKey, countInfo) =>
        //根据实体类来构建数据
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        var clickCount = sortKey.clickCount
        var orderCount = sortKey.orderCount
        var payCount = sortKey.payCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }


    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10SortKey2FullCountArray
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   order2clickCountRDD: RDD[(Long, Long)],
                   pay2clickCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD: RDD[(Long, String)] = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrCount)
    }

    val cid2OrderInfoRDD: RDD[(Long, String)] = cid2ClickInfoRDD.leftOuterJoin(order2clickCountRDD).map {
      //todo 注意这里因为之前的追加变成了clickInfo
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo +"|"+ Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (cid, aggrInfo)
    }
    val cid2PayInfoRDD: RDD[(Long, String)] = cid2OrderInfoRDD.leftOuterJoin(pay2clickCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val payInfo = orderInfo+"|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, payInfo)
    }
    cid2PayInfoRDD
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    var orderFilterRDD = sessionId2FilterActionRDD.filter{
      case (session,action) =>
        action.order_category_ids != null
    }
    val orderNumRDD: RDD[(Long, Long)] = orderFilterRDD.flatMap {
      case (sessionId, action) =>
        action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    orderNumRDD.reduceByKey(_+_)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFliterRDD = sessionId2FilterActionRDD.filter{
      case (sessionId,action) =>
       action.pay_category_ids != null
    }
    // action.pay_category_ids.split(","):Array[String]
    //  action.pay_category_ids.split(",").map(item => (item.toLong,1L))
    //todo 注意这里是利用先将我们的字符串拆分成字符串数组，然后使用map转化数组中的每个元素，
    //todo 原来我们每一个元素都是一个string，现在转化为（long，1L）
    var payNumRDD: RDD[(Long, Long)] = payFliterRDD.flatMap{
      case (sessionId,action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong,1L))
    }
    payNumRDD.reduceByKey(_+_)
  }
  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)])= {
//    val clickFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
//      case (sessionId, action) =>
//        action.click_category_id != -1
//    }
    //过滤操作 
    //todo 注意过滤的时候只需要判断点击，下单和付款对应的categoryid是否为空
    val clickFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1)

    //转化，为reduceBykey做准备
    val clickNumRDD: RDD[(Long, Long)] = clickFilterRDD.map {
      case (sessionId, action) =>
        (action.click_category_id, 1L)
    }
    //执行reduceBykey的操作
    clickNumRDD.reduceByKey(_+_)
  }

  def generateRandomIndexList(extractPerDay:Long, //一天一共要抽取的数量
                              daySessionCount:Long,  //这一天一共有多少个
                              hourCountMap:mutable.HashMap[String,Long],  //每个小时有多少个
                              hourListMap:mutable.HashMap[String,ListBuffer[Int]]): Unit ={
    for ((hour,count) <- hourCountMap){
      //即当前这个小时要抽取的个数
      var hourExrCount: Int = ((count/daySessionCount.toDouble)*extractPerDay).toInt
      //避免一个小时要抽取的书来给你超过这个小时的总数
      if (hourExrCount > count){
        hourExrCount = count.toInt
      }

      val random = new Random()

      //hourListMap [hour, {1,2,4,5,6}]  即通过随机数生成要抽取的索引列表
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount){
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }

        case Some(list) =>
          for (i <- 0 until hourExrCount){
            //todo 注意这里随机数范围取的是 count的个数
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }
    }
  }

  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]): Unit = {
    //dateHourFullInfoRDD：RDD[(dateHour，fullinfo)]
    val dateHour2FullInfoRDD: RDD[(String, String)] = sessionId2FilterRDD.map {
      case (sid, fullinfo) =>
        val startTime: String = StringUtils.getFieldFromConcatString(fullinfo, "\\|", Constants.FIELD_START_TIME)
        val dateHour: String = DateUtils.getDateHour(startTime)
        (dateHour, fullinfo)
    }

    //hourCountMap: Map[dateHour,count]
    val hourCountMap: collection.Map[String, Long] = dateHour2FullInfoRDD.countByKey()
   //dateHourCountMap: Map[date,Map[hour,count]]
    val dateHourCountMap = new mutable.HashMap[String,mutable.HashMap[String,Long]]()

    for ((dateHour,count) <- hourCountMap){
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
          //这个时候的结构是 Map[date,[null,null]]
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }

    //解决问题1 一共有多少天
    //获取map中有多少key
    val size = dateHourCountMap.size
    //一天要抽取的数量
    val extractPerDay = 100/size

    //解决问题二 一天有多少个session dateHourCountMap(date).values.sum
    //解决问题三 一个小时有多少条session dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]()

    //dateHourCountMap: Map[date,Map[hour,count]]
    for ((date,hourCountMap) <- dateHourCountMap){
      //这一天一共有多少个session
      val dateSessionCount: Long = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String,ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay,dateSessionCount,hourCountMap,dateHourExtractIndexListMap(date))

        case Some(map)  =>
          generateRandomIndexList(extractPerDay,dateSessionCount,hourCountMap,dateHourExtractIndexListMap(date))

      }

      //到目前为止获取了一天中每个小时要抽取的session个数

      //将变量声明为广播大变量，提升任务性能
      val dateHourExtractIndexListMapBd: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)
      //dateHourFullInfoRDD：RDD[(dateHour，fullinfo)]
      //即 将dateHourFullInfoRDD 聚合成斧子型数据后，与随机数生成要抽取的索引列表相匹配
      val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullInfoRDD.groupByKey()

      val extractSessionRDD: RDD[SessionRandomExtract] = dateHour2GroupRDD.flatMap {
        case (dateHour, iterable) => {
          val date = dateHour.split("_")(0)
          var hour = dateHour.split("_")(1)

          val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)

          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for (fullInfo <- iterable) {
            if (extractList.contains(index)) {
              val sessionid = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val starttime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategoryIds = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
              extractSessionArrayBuffer += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
            }
            index += 1
          }
          extractSessionArrayBuffer
        }

      }

      /* 将抽取后的数据保存到MySQL */

      // 引入隐式转换，准备进行RDD向Dataframe的转换
      import sparkSession.implicits._
      // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("dbtable", "session_random_extract")
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .mode(SaveMode.Append)
        .save()


    }

  }



  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    //从累加器中获取值
    val session_count: Double = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    val visitLength_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s,0)
    val visit_length_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s,0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3: Int = value.getOrElse(Constants.STEP_PERIOD_1_3,0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_Length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m/session_count,2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    //todo 创建一个包含上面的全部属性的样例类实例
    var stat = SessionAggrStat(taskUUID,session_count.toLong,
      visit_Length_1s_3s_ratio,visit_length_4s_6s_ratio,visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio,visit_length_30s_60s_ratio,visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD: RDD[SessionAggrStat] = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._

    sessionRatioRDD.toDF().write.format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }


  def getSessionFilterRDD(taskParam: JSONObject,
                          sessionId2FullInfoRDD: RDD[(String, String)],
                          sessionAccumulator: SessionAccumulator) = {
    val startAge: String = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val proFessionals: String = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keyWords: String = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)

    //1 获取过滤信息字符串
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "")+
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else  "") +
      (if (proFessionals != null) Constants.PARAM_PROFESSIONALS + "=" + proFessionals + "|" else  "") +
       (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else  "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else  "") +
      (if (keyWords != null) Constants.PARAM_KEYWORDS + "=" + keyWords + "|" else  "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else  "")

    if (filterInfo.endsWith("\\|"))
      filterInfo.substring(0,filterInfo.length-1)

    sessionId2FullInfoRDD.filter {
     case (sessionId, fullInfo) =>
      var success = true

      if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
        success = false
      } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
        success = false
      } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
        success = false
      } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
        success = false
      } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
        success = false
      } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
        success = false
      }

       //todo 如果数据符合过滤条件
      if(success){
        //将数据总数加1
        sessionAccumulator.add(Constants.SESSION_COUNT)
        //获取访问时长，根据模式匹配找到对应的区间并加1
        val visitLength: Long = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong

        //获取访问步长，根据模式匹配找到对应的区间并加1
        val stepLength: Long = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong
        //计算访问时长范围
        calculateVisitLength(visitLength,sessionAccumulator)

        //计算访问步长范围
        calculateStepLength(stepLength,sessionAccumulator)
      }
      success
    }

  }


  def calculateStepLength(stepLength: Long, sessionAggrStatAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
    } else if (stepLength > 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
    }
  }

  def calculateVisitLength(visitLength: Long, sessionAggrStatAccumulator: SessionAccumulator) = {
    if(visitLength >= 1 && visitLength <= 3){
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if (visitLength >= 4 && visitLength <= 6){
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
    } else if (visitLength > 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
    }
  }
  def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
   //todo  结构：USERId2AggrInfoRDD: RDD[(userId,aggrInfo)]
    val userId2AggreInfoRDD: RDD[(Long, String)] = {
      session2GroupActionRDD.map {
        case (sessionId, iterableAction) =>
          var userId = -1L
          var startTime: Date = null

          var endTime: Date = null

          var stepLength = 0

          var searchKeywords = new StringBuilder("")
          var clickCategories = new StringBuilder("")

          for (action <- iterableAction) {
            if (userId == -1L) {
              userId = action.user_id
            }

            val actionTime: Date = DateUtils.parseTime(action.action_time)
            if (startTime == null || startTime.after(actionTime)) {
              startTime = actionTime
            }

            if (endTime == null || endTime.before(actionTime)) {
              endTime = actionTime
            }

            val searchKeyword: String = action.search_keyword
            if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString().contains(searchKeyword)) {
              searchKeywords.append(searchKeyword + ",")
            }

            val clickCategoryId: Long = action.click_category_id
            if (clickCategoryId != -1 && !clickCategories.toString().contains(clickCategoryId)) {
              clickCategories.append(clickCategoryId + ",")
            }
            stepLength += 1
          }

          StringUtils.trimComma(searchKeywords.toString())
          StringUtils.trimComma(clickCategories.toString())

          val visitLength: Long = (endTime.getTime - startTime.getTime) / 1000

          var aggregateInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
            Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
            Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategories + "|" +
            Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
            Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
            Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

          (userId, aggregateInfo)
      }
    }
    //联立用户表
    val sql = "select * from user_info"

    import sparkSession.implicits._
    
    val userId2InfoDataSet: Dataset[UserInfo] = sparkSession.sql(sql).as[UserInfo]
    val userId2InfoRDD: RDD[(Long, UserInfo)] = userId2InfoDataSet.rdd.map(item => (item.user_id,item))

    val sessionId2FullInfoRDD: RDD[(String, String)] = userId2AggreInfoRDD.join(userId2InfoRDD).map {
      case (userId, (agInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        var fullInfo = agInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|"+
        Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId: String = StringUtils.getFieldFromConcatString(agInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
    }
    sessionId2FullInfoRDD
  }

  def getOriactionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    var sql = "select * from user_visit_action where date>='" + startDate +"' and date<='"+endDate+"'"

    import sparkSession.implicits._

    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
