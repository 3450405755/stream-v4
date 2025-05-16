package com.hwq.gongDan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.bean.DimBaseCategory;
import com.hwq.dws.ikfenci.function.MapOrderAndDetailRateModelFunc;
import com.hwq.dws.ikfenci.function.Orderutil;
import com.hwq.until.DateFormatUtil;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.List;

/**
 * @Package com.hwq.gongDan.OrderOne
 * @Author hu.wen.qi
 * @Date 2025/5/15 9:33
 * @description: 1
 */
public class OrderOne {
    private static List<DimBaseCategory> dim_base_categories ;
    private static Connection connection ;

    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "dws_order_detail", "a2");
        SingleOutputStreamOperator<JSONObject> map = kafkaSource.map(JSON::parseObject);

        KeyedStream<JSONObject, Boolean> order_info = map.keyBy(jsonObject -> !jsonObject.getString("user_id").isEmpty());

        SingleOutputStreamOperator<JSONObject> order_water = order_info.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts_ms");
            }
        }));



        SingleOutputStreamOperator<JSONObject> order_user = order_water.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 创建结果JSON对象，存储提取后的字段
                JSONObject result = new JSONObject();
                result.put("uid", jsonObject.getString("user_id"));
                result.put("create_time", jsonObject.getString("create_time"));
                result.put("category1_name", jsonObject.getString("category1_name"));
                result.put("tm_name", jsonObject.getString("tm_name"));
                result.put("split_total_amount", jsonObject.getDouble("split_total_amount"));
                out.collect(result);
            }
        });


        SingleOutputStreamOperator<JSONObject> order_c1 = order_user.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
//                jsonObject.put("uid", jsonObject.getString("uid"))

                //日期处理加权
                Long createTime = jsonObject.getLong("create_time");
                String hourStr = DateFormatUtil.tsToDateTime(createTime).split(" ")[1].split(":")[0];
                int hour = new Integer(hourStr);
                getTimeType(jsonObject, hour);

                //金额区间加权
                Double price = jsonObject.getDouble("split_total_amount");
                if (price <= 1000) {
                    jsonObject.put("price", "低价商品");
                } else if (price > 1000 && price <= 3000) {
                    jsonObject.put("price", "中价商品");
                } else if (price > 3000) {
                    jsonObject.put("price", "高价商品");
                }
                String tmName = jsonObject.getString("tm_name");
                jsonObject.put("tm_name", tmName);
                return jsonObject;
            }
        });

        SingleOutputStreamOperator<JSONObject> order_zui = order_c1.map(new Orderutil());

                SingleOutputStreamOperator<JSONObject> orderFinalDs = order_zui
                .keyBy(jsonObject -> jsonObject.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                 //对窗口内的数据进行规约，保留最后一条记录（丢弃其他记录）
                 //这里使用lambda表达式 (value1, value2) -> value2 表示只保留第二个值（即最后一条记录）
                .reduce((value1, value2) -> value2);

        SingleOutputStreamOperator<JSONObject> orderFinal = orderFinalDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, search_rate_weight_coefficient, time_rate_weight_coefficient, brand_rate_weight_coefficient));
        orderFinal.print();

//
//                orderFinal
//                        .map(JSONObject::toString)
//                                .addSink(KafkaUtil.getKafkaSink("order_two"));



        env.execute();
    }
    public static void getTimeType(JSONObject value , Integer hour) {
        if (hour>=0 && hour<6){
            value.put("time_type", "凌晨");
        }else if (hour>=6&&hour<9){
            value.put("time_type", "早晨");
        }else if (hour>=9&&hour<12){
            value.put("time_type", "上午");
        }else if (hour>=12&&hour<14){
            value.put("time_type", "中午");
        }else if (hour>=14&&hour<18){
            value.put("time_type", "下午");
        }else if (hour>=18&&hour<22){
            value.put("time_type", "晚上");
        }else {
            value.put("time_type", "夜间");
        }

    }
}
