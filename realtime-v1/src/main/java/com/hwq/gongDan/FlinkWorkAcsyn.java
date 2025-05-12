package com.hwq.gongDan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.hwq.gongDan.FlinkWorkAcsyn
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

        SingleOutputStreamOperator<JSONObject> map = filter.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                return jsonObject;
            }
        });

        map.print();


        env.execute();
    }
}
