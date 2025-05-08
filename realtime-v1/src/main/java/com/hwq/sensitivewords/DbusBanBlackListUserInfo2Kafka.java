package com.hwq.sensitivewords;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.hwq.fuction.FilterBloomDeduplicatorFunc;
import com.hwq.fuction.MapCheckRedisSensitiveWordsFunc;
import com.hwq.until.KafkaUtil;
import com.hwq.until.SinkDoris;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Package com.hwq.sensitivewords.DbusBanBlackListUserInfo2Kafka
 * @Author hu.wen.qi
 * @Date 2025/5/8 15:11
 * @description: 黑名单封禁
 */
public class DbusBanBlackListUserInfo2Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
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


        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "sensitivewords_log", "aq");
        //kafkaSource.print();

        //{"info_original_total_amount":16625.0,"info_activity_reduce_amount":250.0,"commentTxt":"差评：联想Y9000P 2022游戏本，性能一般，散热差，价格高。","info_province_id":4,"info_payment_way":"3501","info_refundable_time":1744888222000,"info_order_status":"1004","info_create_time":1744283422000,"id":4,"spu_id":4,"table":"comment_info","info_operate_time":1744283468000,"info_tm_ms":1745921634228,"op":"r","create_time":1744283468000,"info_user_id":59,"info_op":"r","info_trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H 512G RTX3060 钛晶灰等3件商品","sku_id":15,"server_id":"0","dic_name":"好评","info_consignee_tel":"13577181831","info_total_amount":16375.0,"info_out_trade_no":"814615528949485","appraise":"1201","user_id":59,"info_id":51,"info_coupon_reduce_amount":0.0,"order_id":51,"info_consignee":"邬新","ts_ms":1745921629599,"db":"dev_realtime_v6_wenqi_hu"}
        SingleOutputStreamOperator<JSONObject> map = kafkaSource.map(JSON::parseObject);

        //{"info_original_total_amount":4484.0,"info_activity_reduce_amount":0.0,"commentTxt":"商品质量差，画质模糊，声音小，操作复杂，不值得购买。","info_province_id":18,"info_payment_way":"3501","info_refundable_time":1744886974000,"info_order_status":"1004","info_create_time":1744282174000,"id":3,"spu_id":11,"table":"comment_info","info_operate_time":1744282228000,"info_tm_ms":1745921634228,"op":"r","create_time":1744282228000,"info_user_id":55,"info_op":"r","info_trade_body":"华为智慧屏V55i-J 55英寸 HEGE-550B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 银钻灰 京品家电等3件商品","sku_id":33,"server_id":"0","dic_name":"好评","info_consignee_tel":"13197834788","info_total_amount":4484.0,"info_out_trade_no":"926191829254433","appraise":"1201","user_id":55,"info_id":41,"info_coupon_reduce_amount":0.0,"order_id":41,"info_consignee":"常欣","ts_ms":1745921629599,"db":"dev_realtime_v6_wenqi_hu"}
        //使用布隆过滤器过滤掉kafka重复数据
//        SingleOutputStreamOperator<JSONObject> bloomFilterDs = map.keyBy(data -> data.getLong("order_id"))
//                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

        //{"msg":"差评：联想Y9000P游戏本性能一般，散热差，价格过高。","consignee":"钱玉","violation_grade":"","user_id":14,"violation_msg":"","is_violation":0,"ts_ms":1746408259555}
        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = map.map(new MapCheckRedisSensitiveWordsFunc());

        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                if (jsonObject.getIntValue("is_violation") == 0) {
                    String msg = jsonObject.getString("msg");
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        });

       //secondCheckMap.print();

        secondCheckMap.map(JSON::toJSONString)
                        .sinkTo(SinkDoris.getDorisSink("dws_to_doris","sensitive_words"));


        env.execute();
    }
}
