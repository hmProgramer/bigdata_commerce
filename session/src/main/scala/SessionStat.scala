import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.mutable

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
    sessionId2FilterRDD.foreach(println(_))

    //10 从累加器中获取结果
    getSessionRatio(sparkSession,taskUUID,sessionAccumulator.value)
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
            Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime)

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
