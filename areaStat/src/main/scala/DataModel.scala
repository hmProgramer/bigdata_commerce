/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */


//***************** 输出表 *********************

/**
  *
  * @param taskid
  * @param area
  * @param areaLevel
  * @param productid
  * @param cityInfos
  * @param clickCount
  * @param productName
  * @param productStatus
  */
case class AreaTop3Product(taskid:String,
                           area:String,
                           areaLevel:String,
                           productid:Long,
                           cityInfos:String,
                           clickCount:Long,
                           productName:String,
                           productStatus:String)

/**
  *
  * @param city_id
  * @param click_product_id
  */
case class CityClickProduct(city_id:Long,click_product_id:Long)


case class CityAreaInfo(city_id:Long,city_name:String,area:String)