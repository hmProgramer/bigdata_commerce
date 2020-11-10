import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable



object PageOneStepConvertRate {



  def main(args: Array[String]): Unit = {

    //任务限制条件
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParm: JSONObject = JSONObject.fromObject(jsonStr)
    
    //获取唯一主键
    val taskUUID: String = UUID.randomUUID().toString

    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    //创建sparksession spark客户端
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //1 获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession,taskParm)
//    sessionId2ActionRDD.foreach(println(_))

    //2 获取目标页面切片
    // pageFlowArr ：1，2，3，4，5，6，7
    val pageFlowStr: String = ParamUtils.getParam(taskParm,Constants.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArr ：[1，2，3，4，5，6，7]
    val pageFlowArr: Array[String] = pageFlowStr.split(",")

    //pageFlowArr.slice(0,pageFlowArr.length-1):[1,2,3，4，5，6]
    //pageFlowArr.tail: [2，3，4，5，6，7]
    //  pageFlowArr.slice(0,pageFlowArr.length-1).zip(pageFlowArr.tail) ：[(1,2),(2,3)....]
    val targetPageSplit: Array[String] = pageFlowArr.slice(0, pageFlowArr.length - 1).zip(pageFlowArr.tail).map {
      case (page1, page2) => page1 +"_"+ page2
    }

    //3 对sessionId2Action数据进行groupByKEY 操作
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    //4 对每个session对应的iterable类型的数据按照时间（action_time）进行排序
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap{
      case(sessionId,iterableAction) =>
        val sortList: List[UserVisitAction] = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        //pageList: List[Long]
        val pageList: List[Long] = sortList.map{
          case action => action.page_id
        }

        // 把page_id转化为页面切片形式
        //pageSplit [(1_2),(2_3)....]
        val pageSplit: List[String] = pageList.slice(0,pageList.length-1).zip(pageList.tail).map {
          case (page1,page2) =>page1 + "_" + page2
        }
       
        //过滤
        val pageSplitFilter: List[String] = pageSplit.filter{
          pageSplit => targetPageSplit.contains(pageSplit)
        }

        //转换
        pageSplitFilter.map{
          case pageSplit => (pageSplit,1L)
        }
    }

    // 执行countBykey操作，拿到每个页面切片的总个数
    //pageSplitCountMap ：Map[String, Long]===》Map[pageSplit,count]
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    //获取起始页面page1
    val startPage: Long = pageFlowArr(0).toLong

    val startPageCountRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }
    val startPageCount: Long = startPageCountRDD.count()

    // 根据所有的切片个数信息，计算实际的页面切片转化率大小
    getPageConvert(sparkSession,taskUUID,targetPageSplit,startPageCount,pageSplitCountMap)
  }

  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]): Unit = {
    val pageSplitRatio = new mutable.HashMap[String,Double]()

    var lastPageCount: Double = startPageCount.toDouble

    for (pageSplit <- targetPageSplit){
      val currentPageSplitCount: Double = pageSplitCountMap.get(pageSplit).get.toDouble
      val ratio: Double = currentPageSplitCount/lastPageCount
      pageSplitRatio.put(pageSplit,ratio)
      lastPageCount = currentPageSplitCount.toDouble
    }

    //拼接pageSplit 与每部分的切片转换率
    val convertStr: String = pageSplitRatio.map {
      case (pageSplit, ratio) =>
        pageSplit + "=" + ratio
    }.mkString("|")

   val pageSplit = PageSplitConvertRate(taskUUID,convertStr)
    //todo 注意这里要将字符串包裹成Arr 然后转换成rdd
    val pageSplitRatioRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }




  def getUserVisitAction(sparkSession: SparkSession, taskParm: JSONObject) = {
      val startDate: String = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
      val endDate: String = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)

      import sparkSession.implicits._
      val sql = "select * from user_visit_action where date >='" + startDate + "' and date <='" + endDate + "'"
      sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))

//    import sparkSession.implicits._
//    sparkSession.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
//      .as[UserVisitAction].rdd


  }
}
