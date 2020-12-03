项目介绍
    该大数据分析平台对电商网站的各种用户行为（访问行为、购物行为、广告点
    击行为等）进行分析，根据平台统计出来的数据，辅助公司中的 PM（产品经理）、
    数据分析师以及管理人员分析现有产品的情况，并根据用户行为分析结果持续改进 产品的设计，以及调整公司的战略和业务。最终达到用大数据技术来帮助提升公司
    的业绩、营业额以及市场占有率的目标

项目技术
    本项目使用了 Spark 技术生态栈中最常用的三个技术框架，Spark Core、Spark SQL 和 Spark Streaming，以及hive   

离线模拟数据生成
    
    即通过定义动作表，用户信息表，即产品表的相关类及属性，在通过随机数等方式生成模拟数据，并初始化到hive中
    
需求1 用户访问session统计

    1 以开始时间与结束时间为条件获取原始的动作表（user_visit_action）数据 actionRDD
    2 将actionRDD转化为k-v结构 sessionId2ActionRDD                          
    3 对sessionId2ActionRDD  groupBykey 操作  sessionId2GroupRDD (斧子型数据)                         
    4  从sessionId2GroupRDD里的每一天数据提取聚合信息                         
    5 联立user_info表                         
    6 得到最后的每个sessionId对应的完整聚合信息sessionId2FullInfoRDD                              
    7 根据过滤条件对聚合信息进行过滤                                       
    8 根据符合过滤条件的session更新累加器信息                                        
    9 从累加器中读取各个访问步长的session的个数，计算比率     
                                                    
                                                    
2 需求二 随机抽取session
    
    关键点 抽取的数量，抽取的随机性
    1 如何保证抽取的随机性？
        通过计算公式可知每小时的抽取数量N，那么可以生成N个随机数，而n个随机数的列表就是要抽取的session的索引列表。
        所以可以按照hour聚合后的数据，从0开始进行编号，如果session对应的编号存在于索引列表中，那么就抽取此session，否则不抽取 
        
        
3 需求三 top10热门品类
   
    1 第一步：是要获取所有发生过点击，下单，付款的品类 
    2 执行去重    cid2CidRDD = cid2CidRDD.distinct()
    3 第二步：统计品类的点击次数
    4 第三步 统计品类的下单次数
    5 第四步 统计品类的付款次数
    6 第五步，整合点击次数，付款次数，下单次数
    7 实现自定义二次排序key
    8 根据case class来构建数据 导入到数据库
   
4 需求四 top10热门品类的top10活跃session
    
    1 根据top10CategoryArray 获取所有top10热门品类的cid数组
    2 通过sessionId2FilterActionRDD 的filter算子获取所有符合过滤条件的并且点击过top10热门品类的action数据
    3 对上述结果执行groupbykey操作-- val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    4 对上述斧子型数据执行flatmap操作，并且遍历他的value值，通过map来维护他的categoryid的个数，
      最后通过yield进行收集-- yield (cid,sessionId+ "=" + count)，转换成以cid为key，sessionCount为value的RDD
    5 再对上述结果执行groupbykey操作，这样就可以获取cid的所有session的点击次数 
        cid2GroupRDD: RDD[(Long, Iterable[String])] = cid2SessionCountRDD.groupByKey()
    6 在对上述结果执行flatmap操作，并且遍历iterable类型的数据并通过sortWith进行排序且take前10
             case (cid, iterableSessionCount) =>
                val sortList: List[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
                  item1.split("=")(1).toLong > item2.split("=")(1).toLong
                }).take(10)
    7 组装 top10Session数据 并进行入库操作
    
    
5 需求5 计算给定的页面访问流的页面单跳转化率


    1 从user_visit_action表里读取指定时间范围内的用户行为数据
    2 RDD(sessionId，action)
    3 获取目标页面切片
    4 对sessionId2Action数据进行groupByKEY 操作
    5 对每个session对应的iterable类型的数据按照时间（action_time）进行排序
    6 取出排序完成的每个action的page_id信息
    7 把page_id转化为页面切片形式
    8 根据目标页面切片，将不存在于目标页面切片的所有切片过滤掉
    9 RDD(pageSplit，1L)
    10 执行countBykey操作，拿到每个页面切片的总个数
    11 获取起始页面page1
    12 得到起始页面个数
    13 根据所有的切片个数信息，计算实际的页面切片转化率大小
    14 封装到case class，写入到mysql数据库
    
    
需求6
    
    1 从user_visit_action 表里，获取city_id和pid信息 RDD(cityId，pid)
    
    2 从array里获取area，city_id,city_name数据
    
    3 聚合 (area,city_id,city_name,pid)
    
    4 创建基本信息表(tmp_area_basic_info)
    
    5 统计每个区域每个广告的点击次数 (tmp_area_click_count)
    
    6 获得拥有完整商品信息的点击次数表 (tmp_area_count_product_info)

    7 统计area  top3的热门品类
    
    8 写入mysql    
    
    
需求7 广告点击黑名单实时统计

    1 
    
需求8 各省各城市一天中的广告点击量

需求9 每天每个省份top3热门广告
   
    1 将各省各城市广告点击量实时统计的DStream 转换成 key-value格式  --》  (newKey,count)
    2 执行reduceBykey的操作
    3 再将将key2ProvinceCityDStream转成touple   
    4 在将touple类型的数据转成一张临时表
    5 最后执行 sparkSession.sql(sql).rdd    ---》top3DSteam  【RDD[row]】
    6 最后再遍历上面的每一个rdd【row】类型 执行入库操作
 
需求10 最近一个小时广告点击量统计 
（通过SprakStreaming 的窗口操作 reduceBykeyAndWindow 实现统计一个小时内每个广告每分钟的点击量）
    
    1 将原始的adRealTimeFilterDStream  转换成key -value格式
    2 执行reduceByKeyAndwindow 操作
    3 写入mysql数据库
    
    
                                    