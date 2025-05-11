package com.hwq.test;

import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.hwq.test.Test
 * @Author hu.wen.qi
 * @Date 2025/5/10 10:26
 * @description: 1
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "topic_log", "ac");
        kafkaSource.print();


        env.execute();
    }
}
