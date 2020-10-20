import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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

//    session2GroupActionRDD.foreach(println(_))
    val userIdAggreInfoRDD: RDD[(Long, String)] = getSessionFullInfo(sparkSession,session2GroupActionRDD)

    userIdAggreInfoRDD.foreach(println(_))
  }

  def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
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
    userId2AggreInfoRDD
  }

  def getOriactionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    var sql = "select * from user_visit_action where date>='" + startDate +"' and date<='"+endDate+"'"

    import sparkSession.implicits._

    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
