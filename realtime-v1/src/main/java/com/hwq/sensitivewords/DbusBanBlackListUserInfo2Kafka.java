package com.hwq.sensitivewords;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.Constant.Constant;
import com.hwq.until.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.Connection;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.hwq.sensitivewords.DbusBanBlackListUserInfo2Kafka
 * @Author hu.wen.qi
 * @Date 2025/5/7 16:49
 * @description: 1
 */
public class DbusBanBlackListUserInfo2Kafka {

    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka读取 评论表 取数
        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "log_topic", "a2");

        //过滤
        SingleOutputStreamOperator<String> kafka_filter = kafkaSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean b = JSON.isValid(s);
                if (b == false) {
                    return false;
                }
                JSONObject table = JSON.parseObject(s).getJSONObject("table");
                if (table != null) {
                    return false;
                }
                return true;
            }});

        //水位线
        SingleOutputStreamOperator<String> tsMs = kafka_filter.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                JSONObject jsonObject = JSON.parseObject(s);
                return jsonObject.getLong("ts_ms");
            }
        }));


        //订单
        SingleOutputStreamOperator<JSONObject> orderinfoStream = tsMs.map(JSON::parseObject)
                .filter(o ->o.getJSONObject("source").getString("table").equals("order_info"));

        //评论
        KeyedStream<JSONObject, String> commentinfoStream = tsMs.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(o -> o.getJSONObject("after").getString("appraise"));


        AsyncDataStream.unorderedWait(
                commentinfoStream,
                new DimAsync<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                    }

                    @Override
                    public String getTableName() {
                        return null;
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getJSONObject("after").getString("appraise");
                    }
                },10,TimeUnit.SECONDS

        ).print();



        env.execute();
    }
}
