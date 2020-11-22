import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * udf 即对缓冲区的数据不断聚合累加
  */
class GroupConcatDistinct extends UserDefinedAggregateFunction{
  //UDAF : 输入数据类型为String
  override def inputSchema: StructType = StructType(StructField("cityInfo",StringType)::Nil)

  //缓冲区类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo",StringType)::Nil)

  //输出数据类型
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo: String = buffer.getString(0)
    var cityInfo: String = input.getString(0)

    if (!bufferCityInfo.contains(cityInfo)){
      if ("".equals(bufferCityInfo)){
        bufferCityInfo += cityInfo
      }else{
        bufferCityInfo += ","+ cityInfo
      }
      buffer.update(0,bufferCityInfo)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1: String = buffer1.getString(0)

    val bufferCityInfo2: String = buffer2.getString(0)

    for (cityInfo <- bufferCityInfo2.split(",")){
      if("".equals(bufferCityInfo1)){
        bufferCityInfo1 += cityInfo
      }else{
        bufferCityInfo1 += ","+cityInfo
      }
    }
    buffer1.update(0,bufferCityInfo1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
