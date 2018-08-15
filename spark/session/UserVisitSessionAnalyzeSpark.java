package com.qf.sessionanalyze1706.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qf.sessionanalyze1706.constant.Constants;
import com.qf.sessionanalyze1706.dao.ISessionAggrStatDAO;
import com.qf.sessionanalyze1706.dao.ITaskDAO;
import com.qf.sessionanalyze1706.dao.factory.DAOFactory;
import com.qf.sessionanalyze1706.domain.SessionAggrStat;
import com.qf.sessionanalyze1706.domain.Task;
import com.qf.sessionanalyze1706.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * 获取用户访问sesison 数据进行分析
 * 1，接收使用者创建的任务信息
 * 任务中过滤条件有：
 * 时间范围：起始时间-结束时间
 * 年龄范围。
 * 性别
 * 职业
 * 所在城市
 * 用户搜索的关键字
 * 点击品类
 * 点击商品
 * 2，spark作业是如何接收使用者创建的任务信息
 *  1)shell脚本通知-调用 spark-submit 脚本执行
 *  2)mysql 的task表中根据指定的taskID 来获取任务信息
 * spark作业开始数据分析
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        /**
         * 模板代码
         */
        // 创建配置信心类

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                .setAppName("UserVisitSessionAnalyzeSpark");
        SparkUtils.setMaster(conf);
        //创建集群入口类
        JavaSparkContext sc = new JavaSparkContext(conf);
        // sparkSql的上下文对象
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //设置检查点
       // sc.checkpointFile("hdfs://hadoop1:9000/...");

        //生成模拟数据
        SparkUtils.mockData(sc, sqlContext);


        //创建获取任务信息的实例
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //获取指定的任务，需要用到taskID
        Long taskID = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);

        // 根据taskID 获取任务信息
        Task task = taskDAO.findById(taskID);

        if (task == null) {
            System.out.println(new Date() + "给的taskID 不能获取到信息");
        }
        // 根据task 去的task_param 字段去获取对应的任务信息
        // task_param 字段量存的就是使用者提供的查询条件
        JSONObject taskParam = JSON.parseObject(task.getTaskParam());

        // 开始查询指定日期范围内的行为数据  【点击，搜索，下单，支付】
        // 首先从  user_visit_action  hive表中查询出指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

       // System.out.println(actionRDD.collect().toString());

        //生成 session粒度的基础数据，得到格式为：<sessionID,actionRdD>
        // row 可以封装任何类型
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ACtionRDD(actionRDD);

        // 对于以后经常用到的数据，最好缓存起来，这样便于以后快速的获取该数据。
        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        // 对行为数据进行聚合
        //1、将行为数据按照sessionID 进行分组
        //2、行为数据RDD需要把用户信息获取到，此时需要用到join，这样得到session粒度的明细数据
        // 明细数据包含了用户基本信息：   拼接字符串优化，节省内存
        // 生成的格式为 <sessionId,(sessionId,searchKeywords,clickCategoryIds,age,professional,city,sex,))
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggrgateBySession(sc, sqlContext, sessionId2ActionRDD);
             //  System.out.println(sessionId2AggrInfoRDD.collect().toString());

        //实现Accmulator 累加器对数据字段进行累加。java + scala 两种方式的accmu
        Accumulator<String> sessionAggrStatAccumulator =
                sc.accumulator("", new SessionAggrStatAccumulator());

        //以session 粒度的数据进行聚合，按照使用者指定的筛选条件进行过滤
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD =
                filteredSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        System.out.println(filteredSessionId2AggrInfoRDD.collect().toString());
        // 缓存过滤后的数据
        //filteredSessionId2AggrInfoRDD =filteredSessionId2AggrInfoRDD.cache();
        filteredSessionId2AggrInfoRDD =filteredSessionId2AggrInfoRDD.persist(
                StorageLevel.MEMORY_AND_DISK() // 存储等级，内+存磁盘
        );
        // 生成一个公共的RDD 通过赛选条件过滤出处理的session得到访问明细

        JavaPairRDD<String,Row> sessionId2DetailRDD=getSessionId2DetailRdd(
                filteredSessionId2AggrInfoRDD,
                sessionId2ActionRDD
        );
        // 缓存
        sessionId2DetailRDD =sessionId2ActionRDD.cache();

        // 如果将上一个聚合的统计结果写入数据库，必须调用action算子触发才能真正执行任务，从Accumulator中获取数据。
        System.out.println(sessionId2DetailRDD.count());

        // 计算出各个范围的session占比，并写入数据库
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());

        // 按照时间比例随机收取sessio
        /**
         * 1、首先计算出每个小时的sesison数量
         * 2，计算出每个小时session数量在一天的比例
         *      例如：要去除100条sesion数据
         *      当前小时sessiong量= 每小时session数据量/sessiong 数据总量   *100
         * 3、按照比例进行随机收取
         */
        randomExtranctSession(sc,task.getTaskid(),filteredSessionId2AggrInfoRDD,sessionId2DetailRDD);


        sc.stop();
    }

    private static void randomExtranctSession(JavaSparkContext sc,
        long taskid,
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD,
         JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * 第一步：计算出每个小时的session数量
         * s首先把数据调整为：<date_hour,data>
         */
        JavaPairRDD<String,String> time2SessionIdRDD=
                filteredSessionId2AggrInfoRDD.mapToPair(
                        new PairFunction<Tuple2<String, String>, String, String>() {
                            @Override
                            public Tuple2<String, String> call(Tuple2<String, String> tup) throws Exception {
                                // 获取聚合数据
                                String aggrInfo =tup._2;
                                //从聚合数据里拿到startTime
                                String startTime =StringUtils.getFieldFromConcatString(
                                        aggrInfo,"\\|",Constants.FIELD_START_TIME
                                );
                                //获取日期和时间
                                String dateHour =DateUtils.getDateHour(startTime);
                                return new Tuple2<String, String>(dateHour,aggrInfo);
                            }
                        });
                        // 需要得到每天每小时的sesion数量，然后计算出每天每小时session抽取索引，遍历每天每小时的session

                        // 首先抽取session聚合数据，写入数据库表
                        //  time2SessionIdRDD   是每天某个小时的session 聚合数据

                    // 计算每天每小时的session 的数量。  ??/
                      Map<String, Object> countMap = time2SessionIdRDD.countByKey();
        /**
         * 第二步：使用时间比例随机抽取算法，计算出每天每小时抽取的session索引。
         */

    }

    private static void calculateAndPersistAggrStat(String value, long taskid) {
       //首先从Accumulator统计的字符串结果中获取各个值
       long session_count=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.SESSION_COUNT));
       long visit_length_1s_3s =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30m));

        long step_length_1_3 =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_1_3));
        long step_length_4_6 =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_4_6));
        long step_length_7_9=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_7_9));
        long step_length_10_30 =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_10_30));
        long step_length_30_60 =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_30_60));
        long step_length_60 =Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_60));

        double visit_length_1s_3s_ratio=  NumberUtils.formatDouble((double)visit_length_1s_3s /(double)session_count,2);
        double  visit_length_4s_6s_ratio=  NumberUtils.formatDouble((double)visit_length_4s_6s /(double)session_count,2);
        double  visit_length_7s_9s_ratio=  NumberUtils.formatDouble((double)visit_length_7s_9s /(double)session_count,2);
        double  visit_length_10s_30s_ratio=  NumberUtils.formatDouble((double)visit_length_10s_30s /(double)session_count,2);
        double  visit_length_30s_60s_ratio=  NumberUtils.formatDouble((double)visit_length_30s_60s /(double)session_count,2);
        double visit_length_1m_3m_ratio=  NumberUtils.formatDouble((double)visit_length_1m_3m /(double)session_count,2);
        double visit_length_3m_10m_ratio=  NumberUtils.formatDouble((double)visit_length_3m_10m /(double)session_count,2);
        double visit_length_10m_30m_ratio=  NumberUtils.formatDouble((double)visit_length_10m_30m /(double)session_count,2);
        double visit_length_30m_ratio=  NumberUtils.formatDouble((double)visit_length_30m /(double)session_count,2);
        double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装到Domain对象里
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 结果存储
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);


    }

    /**
     * 获取通过筛选条件的session的访问明细数据
     * @param filteredSessionId2AggrInfoRDD
     * @param sessionId2ActionRDD
     * @return
     */
    private static JavaPairRDD<String,Row> getSessionId2DetailRdd(
            JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {
        // DE得到sessionid对应的按照使用者条件过滤后的明细数据
        JavaPairRDD<String,Row> sessionId2DetailRDD=
                filteredSessionId2AggrInfoRDD.join(sessionId2ActionRDD)
                        .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                            @Override
                            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tup) throws Exception {
                                return new Tuple2<String, Row>(tup._1,tup._2._2);
                            }
                        }
                );
        return  sessionId2DetailRDD ;
    }

    private static JavaPairRDD<String, String> filteredSessionAndAggrStat(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {
        //先把所有筛选条件提取出来，并拼接为字符串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_END_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categorys != null ? Constants.PARAM_CATEGORY_IDS + "=" + categorys + "|" : "");
        // 把_parameter 的值的最后一个| 去掉
        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;
        // 根据晒选条件进行过滤
        JavaPairRDD<String, String> filteredSessionAggrInfoRDD =
                sessionId2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tup) throws Exception {
                        // 从tup中获取基础数据
                        String aggrInfo = tup._2;
                        /**
                         * 一次按照赛选条件进行过滤
                         * 按照年龄
                         */
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter,
                                Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        //按照职业进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }
                        // 按照性别进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter,
                                Constants.PARAM_SEX)) {
                            return false;
                        }
                        // 按照城市信息进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter,
                                Constants.PARAM_CITIES)) {
                            return false;
                        }
                        // 按照搜索关键词进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter,
                                Constants.PARAM_KEYWORDS)) {
                            return false;
                        }
                        // 按照点击品类进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
                                Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }
                        /**
                         * 代码执行到这里，说明该session通过了用户指定的筛选条件
                         * 接下来要对session的访问时长和访问步长进行统计
                         */
                        // 根据session对应的时长和步长的时间范围进行累加操作

                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围进行累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                        // 计算访问时长范围
                        caculateVisitLength(visitLength);

                        // 计算步长范围
                        caculateStepLength(stepLength);

                        return true;
                    }

                    private void caculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength < 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength >= 30 && stepLength < 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength >= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }

                    }

                    private void caculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength < 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength >= 30 && visitLength < 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength >= 60 && visitLength < 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength >= 180 && visitLength < 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength >= 600 && visitLength < 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength >= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }

                    }
                });

        return filteredSessionAggrInfoRDD;
    }

    private static JavaPairRDD<String, String> aggrgateBySession(
            JavaSparkContext sc,
            SQLContext sqlContext,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {
        // 对行为数据进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionPairRDD =
                sessionId2ActionRDD.groupByKey();

        // 对每个session分组进行聚合，将session中所有的搜索关键字和点击品类都聚合起来
        // 格式：<userId,parAggrInfo(sessionId,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionId2ActionPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                String sessionId = tup._1;
                Iterator<Row> it = tup._2.iterator();
                // 用来存储搜索关键字和品类
                StringBuffer searchKeywordsBuffer = new StringBuffer();
                StringBuffer clickCateGoryIdsBuffer = new StringBuffer();
                // 用来存储userId
                Long userId = null;//大写的long  能存 null
                // 用来存储起始时间和结束时间
                Date startTime = null;
                Date endTime = null;
                // 用来存储访问步长session
                int stepLength = 0;
                // 遍历session中所有数据
                while (it.hasNext()) {
                    Row row = it.next();
                    if (userId == null) {
                        userId = row.getLong(1);
                    }
                    // 获取每个访问行为的搜索关键字和点击品类
                    // 注意：如果该行为是搜索行为，searchKeywords 是有值的，但同时点击行为就没有值，任何的行为，不可能两个字段都有值
                    String searchkeyword = row.getString(5);
                    String clickCtegoryId = String.valueOf(row.getLong(6));
                    //追加 搜索关键字
                    if (!StringUtils.isEmpty(searchkeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchkeyword)) {
                            searchKeywordsBuffer.append(searchkeyword + ",");
                        }
                    }

                    // 追加点击品类
                    if (clickCtegoryId != null) {
                        if (!clickCateGoryIdsBuffer.toString().contains(clickCtegoryId)) {
                            clickCateGoryIdsBuffer.append(clickCtegoryId + ",");
                        }
                    }

                    //计算session 的开始时间，结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }

                    //计算访问步长
                    stepLength++;
                }

                // 截取拼接字符串中两端的“，” 得到搜索关键字和点击品类
                String searckKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCateGoryIdsBuffer.toString());

                // 计算访问时长，单位为妙
                long visitLenth = (endTime.getTime() - startTime.getTime());


                // 聚合数据，数据已字符串拼接的方式
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searckKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLenth + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + startTime + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + DateUtils.formatTime(startTime) + "|" ;


                return new Tuple2<Long, String>(userId, partAggrInfo);
            }
        });//一段段的 mapToPari

        // 查询所有用户的数据，构建成<userId,Row> 格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        // 构建成k-v
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {

                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });
        // 将session粒度的聚合数据和用户信心进行join 格式：<userId,<sessionInfo,userInfo>>

        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD =
                userId2PartAggrInfoRDD.join(userId2InfoRDD);


        // 对join后的数据进行重新拼接，返回格式：<sessionId ,fullAggrInfo>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tup) throws Exception {
                        // 获取sessionId  对应的数据
                        String partAggrInfo = tup._2._1;
                        //获取用户信息
                        Row userInfoRow = tup._2._2;
                        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                        // 提取用户信息的age
                        int age = userInfoRow.getInt(3);
                        // 提取用户信息的职业
                        String professional = userInfoRow.getString(4);
                        // 提取用户信息的城市
                        String city = userInfoRow.getString(5);
                        // 提取用户信息的性别
                        String sex = userInfoRow.getString(6);
                        //拼接
                        String fullAggrInfo = partAggrInfo
                                + Constants.FIELD_AGE + "=" + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex + "|";
                        return new Tuple2<String, String>(sessionId, fullAggrInfo);
                    }
                });
        return sessionId2FullAggrInfoRDD;
    }

    /*
       获取sessionId 对应的行为数据，生成session粒度的数据
     */
    private static JavaPairRDD<String, Row> getSessionId2ACtionRDD(
            JavaRDD<Row> actionRDD) {

        //拿出分区数据，
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> it) throws Exception {
                // 用来封装基础数据
                ArrayList<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
                while (it.hasNext()) {
                    Row row = it.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                    //一个分区的sessionid  放入list中
                }
                return list;
            }
        });
    }
}
