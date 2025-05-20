package com.hwq.gongDan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc;
import com.hwq.dws.ikfenci.function.MapDeviceAndSearchMarkModelFunc;
import com.hwq.fuction.AggregateUserDataProcessFunction;
import com.hwq.fuction.AggregateUserDataProcessFunction1;
import com.hwq.bean.DimBaseCategory;
import com.hwq.fuction.ProcessFilterRepeatTsData;
import com.hwq.until.JdbcUtils;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @Package com.hwq.gongDan.FlinkWorkOrder
 * @Author hu.wen.qi
 * @Date 2025/5/12 9:30
 * @description: 1
 */
public class UserInformationEquipment {

    // 存储所有维度分类信息的列表
    private static final List<DimBaseCategory> dim_base_categories;
    // 数据库连接对象（静态共享，整个应用生命周期只创建一次）
    private static final Connection connection;
    // 设备权重系数（用于后续计算，如用户画像或推荐算法）

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    // 搜索权重系数（用于后续计算，如搜索结果排序或用户行为分析）
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    // 静态代码块，在类加载时执行，初始化数据库连接和维度数据
    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306",
                   "root",
                    "root");
            // SQL查询语句：关联三级分类表，获取完整分类层级信息
            // 包含：三级分类ID、三级分类名称、二级分类名称、一级分类名称
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from dev_realtime_v6_wenqi_hu.base_category3 as b3  \n" +
                    "     join dev_realtime_v6_wenqi_hu.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join dev_realtime_v6_wenqi_hu.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            // 执行SQL查询，将结果映射到DimBaseCategory类的对象列表中
            // false参数表示不关闭数据库连接（由静态变量持有，后续复用）
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> kafka_source_log = KafkaUtil.getKafkaSource(env, "topic_log", "aa");


        //TODO 日志搜索词
        SingleOutputStreamOperator<String> filter = kafka_source_log.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean b = JSON.isValid(s);
                if (b == false) {
                    return false;
                }
                JSONObject jsonObject = JSON.parseObject(s);
                String s1 = jsonObject.getJSONObject("common").getString("uid");
                if (s1 == null) {
                    return false;
                }
                return true;
            }
        });


        SingleOutputStreamOperator<JSONObject> map = filter.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> log_water = map.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        SingleOutputStreamOperator<JSONObject> log_sou = log_water.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 创建结果JSON对象，存储提取后的字段
                JSONObject result = new JSONObject();
                if (!jsonObject.getJSONObject("common").isEmpty() && jsonObject.containsKey("common")) {
                    // 提取公共信息字段
                    JSONObject common = jsonObject.getJSONObject("common");
                    result.put("uid", common.getString("uid"));
                    result.put("ar", common.getString("ar"));
                    result.put("ba", common.getString("ba"));
                    result.put("ch", common.getString("ch"));
                    result.put("md", common.getString("md"));
                    String os = common.getString("os").split(" ")[0];
                    result.put("os", os);
                    result.put("vc", common.getString("vc"));
                    result.put("ts", jsonObject.getString("ts"));
                    // 检查是否包含"page"字段且不为空（通常存放页面浏览信息）
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                        // 提取当前页面的项目名称
                        result.put("item", jsonObject.getJSONObject("page").getString("item"));
                        // 提取页面项目类型（如"keyword"表示搜索关键词）
                        String s = jsonObject.getJSONObject("page").getString("item_type");
                        // 仅当项目类型为"keyword"时，提取关键词并输出结果
                        if ("keyword".equals(s)) {
                            result.put("keyword", jsonObject.getJSONObject("page").getString("item"));
                            out.collect(result);
                        }
                    }
                }
            }
        });

        //log_sou.print();

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = log_sou.filter(jsonObject -> !jsonObject.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(jsonObject -> jsonObject.getString("uid"));

        //keyedStreamLogPageMsg.print();

        //去重 业务逻辑重复触发（如用户重复点击提交按钮），导致生产者生成重复数据。
        SingleOutputStreamOperator<JSONObject> log_distisct = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());


        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = log_distisct.keyBy(data -> data.getString("uid"))
                //使用AggregateUserDataProcessFunction统计每个用户的PV、设备信息和搜索关键词
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                //    对窗口内的数据进行规约，保留最后一条记录（丢弃其他记录）
                //    这里使用lambda表达式 (value1, value2) -> value2 表示只保留第二个值（即最后一条记录）
                .reduce((value1, value2) -> value2);

        //TODO 设备打分模型
        SingleOutputStreamOperator<JSONObject> word_equipment = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));

        //word_equipment.print();







        //TODO 读取业务层用户与用户体重身高
        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "log_topic", "as");
        SingleOutputStreamOperator<String> kafka_filter = kafkaSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject after = jsonObject.getJSONObject("after");
                if (after == null) {
                    return false;
                }
                return true;
            }
        });

        SingleOutputStreamOperator<String> filter_water = kafka_filter.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                JSONObject jsonObject = JSON.parseObject(s);
                return jsonObject.getLong("ts_ms");
            }
        }));

        //TODO 用户信息
        SingleOutputStreamOperator<JSONObject> user_info = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));
         //TODO 用户身高,体重
        SingleOutputStreamOperator<JSONObject> user_info_sup_msg = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        //例如将用户的生日10000转换成2001-01-01
        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = user_info.map(new RichMapFunction<JSONObject, JSONObject>() {

            @Override
            public JSONObject map(JSONObject jsonObject){
                // 从输入的JSON对象中提取"after"字段（通常表示变更后的用户信息）
                JSONObject after = jsonObject.getJSONObject("after");
                // 检查"after"字段是否存在且包含"birthday"属性
                if (after != null && after.containsKey("birthday")) {
                    // 提取生日字段的值（假设存储为Java epochDay格式，即从1970-01-01开始的天数）
                    Integer epochDay = after.getInteger("birthday");
                   // 检查epochDay是否有效（非null）
                    if (epochDay != null) {
                        // 将epochDay转换为LocalDate对象（基于UTC时间，不包含时区）
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        // 将LocalDate格式化为ISO日期字符串（格式：yyyy-MM-dd）
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });




        //TODO  将用户信息数据流转换为包含年龄、年代和星座等计算字段的新数据流
        SingleOutputStreamOperator<JSONObject> user_if = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 创建结果对象，用于存储处理后的用户信息
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    // 提取基本用户信息字段并放入结果对象
                    result.put("uid", after.getString("id"));
                    result.put("uname", after.getString("name"));
                    result.put("user_level", after.getString("user_level"));
                    result.put("login_name", after.getString("login_name"));
                    result.put("phone_num", after.getString("phone_num"));
                    result.put("email", after.getString("email"));
                    result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                    result.put("birthday", after.getString("birthday"));
                    result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                    String birthdayStr = after.getString("birthday");
                    // 处理生日相关计算（年龄、年代、星座）
                    if (birthdayStr != null && !birthdayStr.isEmpty()) {
                        try {
                            // 解析生日字符串为LocalDate对象（ISO格式：yyyy-MM-dd）
                            LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                            // 获取当前日期（使用上海时区，确保时间计算准确）
                            LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                            // 计算年龄（精确到当前日期）
                            int age = calculateAge(birthday, currentDate);
                            // 计算年代（如1980、1990）
                            int decade = birthday.getYear() / 10 * 10;
                            result.put("decade", decade);
                            result.put("age", age);
                            // 计算星座（基于出生日期）
                            String zodiac = getZodiacSign(birthday);
                            result.put("zodiac_sign", zodiac);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                return result;
            }
        });



        //TODO  将用户信息数据流转换为包含身高、体重字段的新数据流
        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = user_info_sup_msg.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                // 创建结果对象，用于存储处理后的用户信息
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("uid", after.getString("uid"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("create_ts", after.getLong("create_ts"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getString("height"));
                    result.put("ts_ms", jsonObject.getLong("ts_ms"));
                }
                return result;
            }
        });

        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = user_if.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));

        //TODO intervalJoin 是一种流处理操作，用于在两个 按键分组的流（KeyedStream） 之间进行 基于时间间隔的连接
        // intervalJoin 能在两个流中，依据设定的时间间隔，找出在时间上存在关联的数据，设定合适的时间间隔，然后将这部分关联成功的数据作为一个新的流（分流）进行后续处理
        SingleOutputStreamOperator<JSONObject> processInter = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());






        SingleOutputStreamOperator<JSONObject> user_Information = processInter
                .keyBy(jsonObject -> jsonObject.getString("uid"))
                .intervalJoin(word_equipment.keyBy(jsonObject -> jsonObject.getString("uid")))
                .between(Time.days(-3), Time.days(3))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        left.put("os", right.getString("os"));
                        left.put("ch", right.getString("ch"));
                        left.put("pv", right.getString("pv"));
                        left.put("md", right.getString("md"));
                        left.put("keyword", right.getString("keyword"));
                        left.put("ba", right.getString("ba"));
                        left.put("b1_category", right.getString("b1_category"));
                        left.put("searchCategory", right.getString("searchCategory"));
                        //设备打分
                        left.put("device_35_39", right.getString("device_35_39"));
                        left.put("device_50", right.getString("device_50"));
                        left.put("device_30_34", right.getString("device_30_34"));
                        left.put("device_18_24", right.getString("device_18_24"));
                        left.put("device_25_29", right.getString("device_25_29"));
                        left.put("device_40_49", right.getString("device_40_49"));
                        left.put("search_25_29", right.getString("search_25_29"));
                        left.put("search_50", right.getString("search_50"));

                        left.put("search_40_49", right.getString("search_40_49"));
                        left.put("search_18_24", right.getString("search_18_24"));
                        left.put("search_35_39", right.getString("search_35_39"));
                        left.put("search_30_34", right.getString("search_30_34"));
                        out.collect(left);
                    }
                });

        user_Information.print();



        SingleOutputStreamOperator<JSONObject> win2Minutes = user_Information.keyBy(data -> data.getString("uid"))
                //使用AggregateUserDataProcessFunction统计每个用户的PV、设备信息和搜索关键词
                .process(new AggregateUserDataProcessFunction1())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                //    对窗口内的数据进行规约，保留最后一条记录（丢弃其他记录）
                //    这里使用lambda表达式 (value1, value2) -> value2 表示只保留第二个值（即最后一条记录）
                .reduce((value1, value2) -> value2);

    win2Minutes.print();

//        win2Minutes
//                .map(JSON::toJSONString)
//                .addSink(KafkaUtil.getKafkaSink("win2_minutes"));



        env.execute();
    }


  private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
            return Period.between(birthDate, currentDate).getYears();
        };

        private static String getZodiacSign(LocalDate birthDate) {
            int month = birthDate.getMonthValue();
            int day = birthDate.getDayOfMonth();

            // 星座日期范围定义
            if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
            else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
            else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
            else if (month == 3 || month == 4 && day <= 19) return "白羊座";
            else if (month == 4 || month == 5 && day <= 20) return "金牛座";
            else if (month == 5 || month == 6 && day <= 21) return "双子座";
            else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
            else if (month == 7 || month == 8 && day <= 22) return "狮子座";
            else if (month == 8 || month == 9 && day <= 22) return "处女座";
            else if (month == 9 || month == 10 && day <= 23) return "天秤座";
            else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
            else return "射手座";
        }
}
