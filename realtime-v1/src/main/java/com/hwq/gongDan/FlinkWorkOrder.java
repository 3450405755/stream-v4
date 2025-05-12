package com.hwq.gongDan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.fuction.FilterBloomDeduplicatorFunc;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.K;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



import java.time.Duration;
import java.time.LocalDate;
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
        DataStream<String> kafka_source_log = KafkaUtil.getKafkaSource(env, "search_term", "aa");

        SingleOutputStreamOperator<String> filter = kafka_source_log.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean b = JSON.isValid(s);
                if (b == false) {
                    return false;
                }
                String string = JSON.parseObject(s).getString("uid");
                if (string == null) {
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


        SingleOutputStreamOperator<JSONObject> order_info = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_info"));


        SingleOutputStreamOperator<JSONObject> order_detail = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_detail"));


        SingleOutputStreamOperator<JSONObject> user_info_sup_msg = filter_water.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));


// 假设 JSON 对象中有一个 "after" 字段包含 user_id
        SingleOutputStreamOperator<JSONObject> order_inter = order_info
                .keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("id"))
                .intervalJoin(order_detail.keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("order_id")))
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        left.put("sku_id", right.getJSONObject("after").getString("sku_id"));
                        left.put("sku_name", right.getJSONObject("after").getString("sku_name"));
                        left.put("order_price", right.getJSONObject("after").getDouble("order_price"));
                        left.put("sku_num", right.getJSONObject("after").getInteger("sku_num"));
                        left.put("split_total_amount", right.getJSONObject("after").getDouble("split_total_amount"));
                        left.put("split_activity_amount", right.getJSONObject("after").getDouble("split_activity_amount"));
                        left.put("split_coupon_amount", right.getJSONObject("after").getDouble("split_coupon_amount"));
                        out.collect(left);
                    }
                });

        SingleOutputStreamOperator<JSONObject> user_inter = order_inter
                .keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("user_id"))
                .intervalJoin(user_info.keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("id")))
                .between(Time.days(-50), Time.days(50))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        left.put("login_name", right.getJSONObject("after").getString("login_name"));
                        left.put("nick_name", right.getJSONObject("after").getString("nick_name"));
                        left.put("name", right.getJSONObject("after").getString("name"));
                        if (right.getJSONObject("after").getString("birthday") != null && right.getJSONObject("after").containsKey("birthday")){
                            left.put("birthday", right.getJSONObject("after").getInteger("birthday"));
                            if (right.getJSONObject("after").getInteger("birthday") !=null){
                                LocalDate date = LocalDate.ofEpochDay(right.getJSONObject("after").getInteger("birthday"));
                                left.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                            }
                        }
                        left.put("gender", right.getJSONObject("after").getString("gender"));
                        out.collect(left);
                    }
                });

        SingleOutputStreamOperator<JSONObject> user_detail = user_inter
                .keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("id"))
                .intervalJoin(user_info_sup_msg.keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("uid")))
                .between(Time.seconds(-1), Time.seconds(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        left.put("height", right.getJSONObject("after").getString("height"));
                        left.put("weight", right.getJSONObject("after").getString("weight"));
                        out.collect(left);
                    }
                });

        SingleOutputStreamOperator<JSONObject> user = user_detail
                .keyBy(jsonObject -> jsonObject.getJSONObject("after").getString("user_id"))
                .intervalJoin(log_water.keyBy(jsonObject -> jsonObject.getString("uid")))
                .between(Time.days(-2), Time.days(2))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject clone = (JSONObject)left.clone();
                            clone.put("ba", right.getString("ba"));
                            clone.put("os", right.getString("os"));
                            clone.put("vc", right.getString("vc"));
                            clone.put("md", right.getString("md"));
                            clone.put("keyword", right.getString("keyword"));
                            clone.put("keyword_count", right.getString("keyword_count"));
                            out.collect(clone);
                    }
                });

        //user.print();

        SingleOutputStreamOperator<JSONObject> bloomFilterDs = user.keyBy(data -> data.getJSONObject("after").getString("user_id"))
               .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

       //bloomFilterDs.print();

        bloomFilterDs
                .map(JSON::toJSONString)
                .addSink(KafkaUtil.getKafkaSink("bloomfilter_log"));


        env.execute();
    }
}
