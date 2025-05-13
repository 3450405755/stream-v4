package com.hwq.gongDan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.dws.ikfenci.function.FlinkWorkOrderFuc;
import com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc;
import com.hwq.fuction.FilterBloomDeduplicatorFunc;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.K;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.hwq.gongDan.FlinkWorkOrder
 * @Author hu.wen.qi
 * @Date 2025/5/12 9:30
 * @description: 1
 */
public class FlinkWorkOrder {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "log_topic", "as");

        //{"stt":"2025-05-11 23:05:50","edt":"2025-05-11 23:06:00","ba":"iPhone","os":"iOS 13.2.9","vc":"v2.1.134","md":"iPhone 14 Plus","ts":1746975956840,"keyword":"衬衫","keyword_count":1}
        DataStream<String> kafka_source_log = KafkaUtil.getKafkaSource(env, "topic_log", "aa");


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
                JSONObject result = new JSONObject();
                if (!jsonObject.getJSONObject("common").isEmpty() && jsonObject.containsKey("common")) {
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
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                        result.put("item", jsonObject.getJSONObject("page").getString("item"));
                        String s = jsonObject.getJSONObject("page").getString("item_type");
                        if ("keyword".equals(s)) {
                            result.put("keyword", jsonObject.getJSONObject("page").getString("item"));
                            out.collect(result);
                        }
                    }
                }
            }
        });

        log_sou.keyBy(jsonObject -> jsonObject.getString("uid"))
        .process(new ProcessFunction<JSONObject, JSONObject>() {
            ValueState<JSONObject> state;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> value1 = new ValueStateDescriptor<>("state", JSONObject.class);
                state = getRuntimeContext().getState(value1);
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = state.value();
                if (jsonObject != null) {
                  state.update(value);
                }
                state.update(value);
                out.collect(value);
            }
        }).print();











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

        SingleOutputStreamOperator<JSONObject> user_info = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));

        //将用户的生日19999转换成2001-01-01
        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = user_info.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });


        SingleOutputStreamOperator<JSONObject> user_info_sup_msg = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        SingleOutputStreamOperator<JSONObject> user_if = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
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
                    if (birthdayStr != null && !birthdayStr.isEmpty()) {
                        try {
                            LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                            LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                            int age = calculateAge(birthday, currentDate);
                            int decade = birthday.getYear() / 10 * 10;
                            result.put("decade", decade);
                            result.put("age", age);
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


        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = user_info_sup_msg.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
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

        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

        //processIntervalJoinUserInfo6BaseMessageDs.print();



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
