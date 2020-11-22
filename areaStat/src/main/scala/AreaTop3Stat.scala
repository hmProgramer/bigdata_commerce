import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

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
    //todo 表中的一条数据就代表一次点击行为
    getAreaPidBasicInfoTable(sparkSession,city2pid,cityId2AreaInfoRDD)

    sparkSession.udf.register("concat_long_string",(v1:Long,v2:String,split:String)=>{
      v1+split+v2
    })

    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinct)
    //4 聚合
    getAreaProductClickCountTable(sparkSession)


    //todo 获取product_info 表中的 extend_info里的 product_status属性字段
    sparkSession.udf.register("get_json_field",(json:String,field:String) =>{
      val jsonObject: JSONObject = JSONObject.fromObject(json) //todo 把json串转成jsonObject
      jsonObject.getString(field)
    })

    getAreaProductClickCountInfo(sparkSession)

    getAreaTop3ProductRDD(sparkSession,taskUUID)


  }

  def getAreaTop3ProductRDD(sparkSession: SparkSession, taskUUID: String)= {
    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end

//    val sql = "SELECT area, CASE WHEN area='华北' OR area='华东' THEN 'A_Level' "+
//    "WHEN area='华中' OR area='华南' THEN 'B_level' " +
//    "WHEN area=西南' OR area='西北' THEN 'C_level' " +
//    "ELSE 'D_level' " +
//    "END area_level, "+
//    "city_infos,pid,product_name,product_status,click_count from ( "+
//    "select area,city_infos,pid,product_name,product_status,click_count, " +
//    "row_number() over (PARTITION BY area ORDER BY click_count DESC) rank from  "+
//    "tmp_area_count_product_info ) t where rank<=3"

    val sql = "SELECT " +
      "area," +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A Level' " +
      "WHEN area='华中' OR area='华南' THEN 'B Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "END area_level," +
      "pid," +
      "city_infos," +
      "click_count," +
      "product_name," +
      "product_status " +
      "FROM (" +
      "SELECT " +
      "area," +
      "pid," +
      "click_count," +
      "city_infos," +
      "product_name," +
      "product_status," +
      "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
      "FROM tmp_area_count_product_info " +
      ") t " +
      "WHERE rank<=3"


    val top3productRDD: RDD[AreaTop3Product] = sparkSession.sql(sql).rdd.map {
      case row =>
        AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("pid"), row.getAs[String]("city_infos"), row.getAs[Long]("click_count"),
          row.getAs[String]("product_name"), row.getAs[String]("product_status"))
    }
    import sparkSession.implicits._

    top3productRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
    val sql = "select tacc.area,tacc.city_infos,tacc.pid,pi.product_name,"+
    "if(get_json_field(pi.extend_info,'product_status')='0','直营','第三方') product_status, tacc.click_count" +
    " from tmp_area_click_count tacc join product_info pi on tacc.pid = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
  }


//  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
//    val sql = "select tacc.area,tacc.city_infos,tacc.pid,pi.product_name,"+
//      "get_json_field(pi.extend_info,'product_status') product_status" +
//      " from tmp_area_click_count tacc join product_info pi on tacc.pid = pi.product_id"
//
//    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
//  }



  def getAreaProductClickCountTable(sparkSession: SparkSession) = {
    val sql = "select area,pid,count(*) click_count," +
    "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos"+
      " from tmp_area_basic_info group by area,pid"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")
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
