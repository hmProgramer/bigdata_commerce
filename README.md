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
                              ----》2 将actionRDD转化为k-v结构 sessionId2ActionRDD
                                 
                                   ----》3 对sessionId2ActionRDD  groupBykey 操作  sessionId2GroupRDD (斧子型数据)
                                 
                                     -----》4  从sessionId2GroupRDD里的每一天数据提取聚合信息
                                 
                                       ----》5 联立user_info表
                                 
                                         --------》6 得到最后的每个sessionId对应的完整聚合信息sessionId2FullInfoRDD
                                    
                                              ----->7 根据过滤条件对聚合信息进行过滤
                                              
                                                ----》8 根据符合过滤条件的session更新累加器信息
                                                
                                                    ---》9从累加器中读取各个访问步长的session的个数，计算比率     
                                    