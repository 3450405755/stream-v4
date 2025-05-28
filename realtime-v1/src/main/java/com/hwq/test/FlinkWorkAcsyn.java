package com.hwq.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.until.DimAsync;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.hwq.test.FlinkWorkAcsyn
 * @Author hu.wen.qi
 * @Date 2025/5/12 20:07
 * @description: 1
 */
public class FlinkWorkAcsyn {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "bloomfilter_log", "av");
        SingleOutputStreamOperator<JSONObject> kafka_jsonobject = kafkaSource.map(JSON::parseObject);
        //kafka_jsonobject.print();

        SingleOutputStreamOperator<JSONObject> filter = kafka_jsonobject.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String after = jsonObject.getString("after");
                if (after == null) {
                    return false;
                }
                return true;
            }
        });

        SingleOutputStreamOperator<JSONObject> water = filter.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts_ms");
            }
        }));



        SingleOutputStreamOperator<JSONObject> map = filter.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //创建一个新对象将数据存进去
                JSONObject object = new JSONObject();
                object.put("user_id",jsonObject.getJSONObject("after").getString("user_id"));
                object.put("name",jsonObject.getString("name"));
                String gender = jsonObject.getString("gender");
                String result = (gender != null)
                        ? (gender.equals("M") ? "M" : "F")
                        : "home";
                object.put("gender",result);
                object.put("era",jsonObject.getString("birthday").substring(3,4)+"0");
                object.put("height",jsonObject.getString("height"));
                object.put("weight",jsonObject.getString("weight"));
                //当前时间
                LocalDate now = LocalDate.now();
                //获取1990-01-01
                String s1 = jsonObject.getString("birthday");
                String[] parts = s1.split("-");
                String year = parts[0];
                String month = parts[1];
                String day = parts[2];
                //获取生日的localdate
                LocalDate y1 = LocalDate.of(new Integer(year),new Integer(month) ,new Integer(day));
                //求生日和当前差
                Period between = Period.between(y1, now);
                //获取差多少年
                int years = between.getYears();
                object.put("age",years);
                //实现参数
                object.put("xingzuo",calculateZodiacSign(y1));
                return object;
            }
        });

       // map.print();


        env.execute();
    }

    public static String calculateZodiacSign(LocalDate date) {
        //月份
        int month = date.getMonthValue();
        //天
        int day = date.getDayOfMonth();
        if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        } else if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else {
            return "双鱼座"; // (month == 2 && day >= 19) || (month == 3 && day <= 20)
        }
    }
}

