/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package commons.pool

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

// 创建用于处理MySQL查询结果的类的抽象接口
trait QueryCallback {
  def process(rs: ResultSet)
}

/**
  * MySQL客户端代理对象
  *
  * @param jdbcUrl      MySQL URL
  * @param jdbcUser     MySQL 用户
  * @param jdbcPassword MySQL 密码
  * @param client       默认客户端实现
  */
case class MySqlProxy(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, client: Option[Connection] = None) {

  // 获取客户端连接对象
  private val mysqlClient = client getOrElse {
    DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
  }

  /**
    * 执行增删改SQL语句
    *
    * @param sql
    * @param params
    * @return 影响的行数
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null

    try {
      // 第一步：关闭自动提交
      mysqlClient.setAutoCommit(false)
      // 第二步：根据传入的sql语句创建prepareStatement
      pstmt = mysqlClient.prepareStatement(sql)

      // 第三步：为prepareStatement中的每个参数填写数值
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      // 第四步：执行增删改操作
      rtn = pstmt.executeUpdate()
      // 第五步：手动提交
      mysqlClient.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  /**
    * 执行查询SQL语句
    *
    * @param sql
    * @param params
    */
  def executeQuery(sql: String, params: Array[Any], queryCallback: QueryCallback) {
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      // 第一步：根据传入的sql语句创建prepareStatement
      pstmt = mysqlClient.prepareStatement(sql)

      // 第二步：为prepareStatement中的每个参数填写数值
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }

      // 第三步：执行查询操作
      rs = pstmt.executeQuery()
      // 第四步：处理查询后的结果
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace
    }
  }

  /**
    * 批量执行SQL语句
    *
    * @param sql
    * @param paramsList
    * @return 每条SQL语句影响的行数
    */
  def executeBatch(sql: String, paramsList: Array[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      // 第一步：关闭自动提交
      mysqlClient.setAutoCommit(false)
      pstmt = mysqlClient.prepareStatement(sql)

      // 第二步：为prepareStatement中的每个参数填写数值
      if (paramsList != null && paramsList.length > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }

      // 第三步：执行批量的SQL语句
      rtn = pstmt.executeBatch()

      // 第四步：手动提交
      mysqlClient.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  // 关闭MySQL客户端
  def shutdown(): Unit = mysqlClient.close()
}

/**
  * 将MySqlProxy实例视为对象，MySqlProxy实例的创建使用对象池进行维护
  */

/**
  * 创建自定义工厂类，继承BasePooledObjectFactory工厂类，负责对象的创建、包装和销毁
  * @param jdbcUrl
  * @param jdbcUser
  * @param jdbcPassword
  * @param client
  */
class PooledMySqlClientFactory(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, client: Option[Connection] = None) extends BasePooledObjectFactory[MySqlProxy] with Serializable {

  // 用于池来创建对象
  override def create(): MySqlProxy = MySqlProxy(jdbcUrl, jdbcUser, jdbcPassword, client)

  // 用于池来包装对象
  override def wrap(obj: MySqlProxy): PooledObject[MySqlProxy] = new DefaultPooledObject(obj)

  // 用于池来销毁对象
  override def destroyObject(p: PooledObject[MySqlProxy]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}

/**
  * 创建MySQL池工具类
  */
object CreateMySqlPool {

  // 加载JDBC驱动，只需要一次
  Class.forName("com.mysql.jdbc.Driver")

  // 在org.apache.commons.pool2.impl中预设了三个可以直接使用的对象池：GenericObjectPool、GenericKeyedObjectPool和SoftReferenceObjectPool
  // 创建genericObjectPool为GenericObjectPool
  // GenericObjectPool的特点是可以设置对象池中的对象特征，包括LIFO方式、最大空闲数、最小空闲数、是否有效性检查等等
  private var genericObjectPool: GenericObjectPool[MySqlProxy] = null

  // 伴生对象通过apply完成对象的创建
  def apply(): GenericObjectPool[MySqlProxy] = {
    // 单例模式
    if (this.genericObjectPool == null) {
      this.synchronized {
        // 获取MySQL配置参数
        val jdbcUrl = ConfigurationManager.config.getString(Constants.JDBC_URL)
        val jdbcUser = ConfigurationManager.config.getString(Constants.JDBC_USER)
        val jdbcPassword = ConfigurationManager.config.getString(Constants.JDBC_PASSWORD)
        val size = ConfigurationManager.config.getInt(Constants.JDBC_DATASOURCE_SIZE)

        val pooledFactory = new PooledMySqlClientFactory(jdbcUrl, jdbcUser, jdbcPassword)
        val poolConfig = {
          // 创建标准对象池配置类的实例
          val c = new GenericObjectPoolConfig
          // 设置配置对象参数
          // 设置最大对象数
          c.setMaxTotal(size)
          // 设置最大空闲对象数
          c.setMaxIdle(size)
          c
        }
        // 对象池的创建需要工厂类和配置类
        // 返回一个GenericObjectPool对象池
        this.genericObjectPool = new GenericObjectPool[MySqlProxy](pooledFactory, poolConfig)
      }
    }
    genericObjectPool
  }
}

