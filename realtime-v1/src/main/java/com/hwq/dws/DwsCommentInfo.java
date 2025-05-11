package com.hwq.dws;

import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.hwq.dws.DwsCommentInfo
 * @Author hu.wen.qi
 * @Date 2025/5/11 20:28
 * @description: 1
 */
public class DwsCommentInfo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
// 精确一次语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 检查点超时时间（2分钟）
        checkpointConfig.setCheckpointTimeout(120000);
// 最小间隔：500毫秒（防止检查点过于频繁）
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
// 最大并发检查点数
        checkpointConfig.setMaxConcurrentCheckpoints(1);


        DataStreamSource<String> order_detail = KafkaUtil.getKafkaSource(env, "dwd_trade_order_detail", "a1");
        //order_detail.print();







        env.execute();
    }
}
