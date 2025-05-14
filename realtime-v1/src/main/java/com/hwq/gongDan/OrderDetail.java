package com.hwq.gongDan;


import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc;
import com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc1;
import com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc2;
import com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc3;
import com.hwq.until.DateFormatUtil;
import com.hwq.until.DimAsync;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.hwq.gongDan.OrderDetail
 * @Author hu.wen.qi
 * @Date 2025/5/14 19:46
 * @description: 1
 */
public class OrderDetail {

    // 默认时间段划分（可根据需求调整）
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "log_topic", "a2");
        SingleOutputStreamOperator<JSONObject> map = kafkaSource.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> filter = map.filter(jsonObject -> jsonObject.getJSONObject("source").getString("table").equals("order_info"));
        SingleOutputStreamOperator<JSONObject> filter1 = map.filter(jsonObject -> jsonObject.getJSONObject("source").getString("table").equals("order_detail"));
        SingleOutputStreamOperator<JSONObject> filter2 = map.filter(jsonObject -> jsonObject.getJSONObject("source").getString("table").equals("sku_info"));
        SingleOutputStreamOperator<JSONObject> filter3 = map.filter(jsonObject -> jsonObject.getJSONObject("source").getString("table").equals("base_trademark"));

        KeyedStream<JSONObject, Boolean> order_info = filter.keyBy(jsonObject -> !jsonObject.getJSONObject("after").getString("id").isEmpty());
        KeyedStream<JSONObject, Boolean> order_detail = filter1.keyBy(jsonObject -> !jsonObject.getJSONObject("after").getString("id").isEmpty());
        KeyedStream<JSONObject, Boolean> sku_info = filter2.keyBy(jsonObject -> !jsonObject.getJSONObject("after").getString("id").isEmpty());
        KeyedStream<JSONObject, Boolean> base_trademark = filter3.keyBy(jsonObject -> !jsonObject.getJSONObject("after").getString("id").isEmpty());

        SingleOutputStreamOperator<JSONObject> order_water = order_info.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts_ms");
            }
        }));

        SingleOutputStreamOperator<JSONObject> order_detail_water = order_detail.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts_ms");
            }
        }));

        SingleOutputStreamOperator<JSONObject> sku_water = sku_info.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts_ms");
            }
        }));

        SingleOutputStreamOperator<JSONObject> base_trademark_water = base_trademark.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts_ms");
            }
        }));


        SingleOutputStreamOperator<JSONObject> map1 = order_water.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.getJSONObject("after")!= null && jsonObject.containsKey("after")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("uid", after.getString("user_id"));
                    result.put("id", after.getString("id"));
                    result.put("create_time", after.getLong("create_time"));
                }
                return result;
            }
        });

        SingleOutputStreamOperator<JSONObject> map2 = order_detail_water.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.getJSONObject("after")!= null && jsonObject.containsKey("after")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("id", after.getString("order_id"));
                    result.put("split_total_amount", after.getDouble("split_total_amount"));
                    result.put("sku_id", after.getString("sku_id"));
                }
                return result;
            }
        });

        SingleOutputStreamOperator<JSONObject> map3 = sku_water.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.getJSONObject("after")!= null && jsonObject.containsKey("after")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("id", after.getString("id"));
                    result.put("tm_id", after.getString("tm_id"));
                }
                return result;
            }
        });

        SingleOutputStreamOperator<JSONObject> map4 = base_trademark_water.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.getJSONObject("after")!= null && jsonObject.containsKey("after")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("tm_name", after.getString("tm_name"));
                    result.put("id", after.getString("id"));
                }
                return result;
            }
        });



        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = map1.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = map2.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc1());

        SingleOutputStreamOperator<JSONObject> finalUserinfoDs1 = processIntervalJs.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs2 = map3.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs1 = finalUserinfoDs1.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs2 = finalUserinfoSupDs2.keyBy(data -> data.getString("id"));

        SingleOutputStreamOperator<JSONObject> processInterval = keyedStreamUserInfoDs1.intervalJoin(keyedStreamUserInfoSupDs2)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc2());

        SingleOutputStreamOperator<JSONObject> finalUserinfoD = processInterval.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupD = map4.filter(data -> data.containsKey("id") && !data.getString("id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoD = finalUserinfoD.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupD = finalUserinfoSupD.keyBy(data -> data.getString("id"));

        SingleOutputStreamOperator<JSONObject> order = keyedStreamUserInfoD.intervalJoin(keyedStreamUserInfoSupD)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc3());

       order.process(new ProcessFunction<JSONObject, JSONObject>() {
           @Override
           public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
               JSONObject result = new JSONObject();
               if (jsonObject!=null){
                   result.put("uid",jsonObject.getString("uid"));
                   result.put("tm_name",jsonObject.getString("tm_name"));
                   Long createTime = jsonObject.getLong("create_time");
                   String s = DateFormatUtil.tsToDateTime(createTime);
                   String date =s.substring(10, 16);
                   result.put("date",date);
                   result.put("split_total_amount",jsonObject.getDouble("split_total_amount"));
                   out.collect(result);
               }
           }
       }).print();





        env.execute();
    }

public static String a1 (int date){
        if ((date > 00  && date <6)) return "凌晨";
        else if (date>=9 && date<12) return "上午";
        else if (date>=12 && date<2) return "中午";
        else if (date>=14 && date<18) return "下午";
        else if (date>=18 && date<21) return "晚上";
        else if (date>=22 && date<24) return "夜间";
        else return "未知";
}
}
