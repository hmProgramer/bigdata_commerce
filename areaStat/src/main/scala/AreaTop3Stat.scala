import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AreaTop3Stat {




  def main(args: Array[String]): Unit = {
    var jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    val taskUUID: String = UUID.randomUUID().toString

    val sparkconf: SparkConf = new SparkConf().setAppName("area").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    //获取了用户的访问行为数据
    //RDD[(cityId,pid)]
    var city2pid = getCityAndProductInfo(sparkSession,taskParam)

    //2 获取城市信息数据
    //RDD[(cityId,CityAreaInfo)]
    var cityId2AreaInfoRDD = getCityArrayInfo(sparkSession)
//    city2pid.foreach(println(_))
    
    //3 根据上面两个rdd获取临时表基础信息表
    getAreaPidBasicInfoTable(sparkSession,city2pid,cityId2AreaInfoRDD)

    sparkSession.sql("select * from tmp_area_basic_info").show()
  }

  def getAreaPidBasicInfoTable(sparkSession: SparkSession,
                               city2pid: RDD[(Long, Long)],
                               cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]):Unit = {
    val areaPidInfoRDD: RDD[(Long, String, String, Long)] = city2pid.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, areaInfo)) =>
        (cityId, areaInfo.city_name, areaInfo.area, pid)
    }

    import sparkSession.implicits._
    areaPidInfoRDD.toDF("city_id","city_name","area","pid").createOrReplaceTempView("tmp_area_basic_info")
  }

  def getCityArrayInfo(sparkSession: SparkSession) = {
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))

    //将array转成rdd
    sparkSession.sparkContext.makeRDD(cityInfo).map{
      case (cityId,cityName,area) =>
        (cityId,CityAreaInfo(cityId,cityName,area))
    }
  }



  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    //只获取发生过点击的action数据
    //一条action数据就代表一个点击行为
    val sql: String = "select city_id,click_product_id from user_visit_action where date >='" + startDate +
    "' and date<='" + endDate + "' and click_product_id != -1"

    import sparkSession.implicits._

    sparkSession.sql(sql).as[CityClickProduct].rdd.map{
      case cityPid => (cityPid.city_id,cityPid.click_product_id)

    }
  }

}
