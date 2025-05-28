package com.hwq.sensitivewords;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.fuction.CommonGenerateTempLate;
import com.hwq.fuction.IntervalJoinOrderCommentAndOrderInfoFunc;
import com.hwq.fuction.SensitiveWordsUtils;
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
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.hwq.sensitivewords.DbusBanBlackListUserInfo2Kafka
 * @Author hu.wen.qi
 * @Date 2025/5/7 16:49
 * @description: 敏感词
 */
public class DbusDBCommentFactData2Kafka {

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


        //异步io
        //{"op":"r","after":{"create_time":1744308362000,"user_id":32,"appraise":"1201","comment_txt":"评论内容：58814254649293852234116621193119582813775777513644","nick_name":"颖颖","sku_id":20,"id":10,"spu_id":6,"order_id":95},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"dev_realtime_v6_wenqi_hu","table":"comment_info"},"dic_name":"好评","ts_ms":1746408259555}
        SingleOutputStreamOperator<JSONObject> commentAsync = AsyncDataStream.unorderedWait(
                commentinfoStream,
                new DimAsync<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        obj.put("dic_name", dimJsonObj.getString("dic_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_dic";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getJSONObject("after").getString("appraise");
                    }
                }, 10, TimeUnit.SECONDS
        );

        //{"op":"r","create_time":1744315696000,"commentTxt":"评论内容：58833611763735165324266645763136165477436568142255","sku_id":26,"server_id":"0","appraise":"1202","user_id":14,"id":13,"spu_id":9,"order_id":117,"ts_ms":1746408259555,"db":"dev_realtime_v6_wenqi_hu","table":"comment_info"}
        SingleOutputStreamOperator<JSONObject> commentobject = commentAsync.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject resJsonObj = new JSONObject();
                Long tsMs = jsonObject.getLong("ts_ms");
                JSONObject source = jsonObject.getJSONObject("source");
                String dbName = source.getString("db");
                String tableName = source.getString("table");
                String serverId = source.getString("server_id");
                if (jsonObject.containsKey("after")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    resJsonObj.put("ts_ms", tsMs);
                    resJsonObj.put("db", dbName);
                    resJsonObj.put("table", tableName);
                    resJsonObj.put("server_id", serverId);
                    resJsonObj.put("appraise", after.getString("appraise"));
                    resJsonObj.put("commentTxt", after.getString("comment_txt"));
                    resJsonObj.put("op", jsonObject.getString("op"));
                    resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                    resJsonObj.put("create_time", after.getLong("create_time"));
                    resJsonObj.put("user_id", after.getLong("user_id"));
                    resJsonObj.put("sku_id", after.getLong("sku_id"));
                    resJsonObj.put("id", after.getLong("id"));
                    resJsonObj.put("spu_id", after.getLong("spu_id"));
                    resJsonObj.put("order_id", after.getLong("order_id"));
                    resJsonObj.put("dic_name", jsonObject.getString("dic_name"));
                    return resJsonObj;
                }
                return null;
            }
        });

        //commentobject.print();

        //{"payment_way":"3501","refundable_time":1744892730000,"original_total_amount":69.0,"order_status":"1002","consignee_tel":"13412413361","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M03赤茶等1件商品","id":42,"operate_time":1744287963000,"op":"r","consignee":"穆卿","create_time":1744287930000,"coupon_reduce_amount":0.0,"out_trade_no":"839946271319251","total_amount":69.0,"user_id":8,"province_id":9,"tm_ms":1746408264214,"activity_reduce_amount":0.0}
        //这行代码定义了一个名为orderInfoMapDs的SingleOutputStreamOperator对象，
        // 它将处理orderinfoStream流中的数据。orderinfoStream是一个包含JSONObject类型数据的流。
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = orderinfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                //从输入的JSONObject中获取操作类型（op）和时间戳（ts_ms）
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                //根据inputJsonObj中是否存在"after"键来决定使用哪个数据对象。如果存在"after"键且不为空，
                // 则使用"after"键对应的JSONObject；否则，使用"before"键对应的JSONObject。
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                //创建一个新的JSONObject对象resultObj，并将操作类型、时间戳和数据对象中的所有键值对添加到resultObj中
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        });


        //分流
        KeyedStream<JSONObject, String> keyedOrderCommentStream = commentobject.keyBy(data -> data.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));

        //{"info_original_total_amount":42231.0,"info_activity_reduce_amount":1199.9,"commentTxt":"评论内容：44591984157473382885872454448236555542219872932183","info_province_id":9,"info_payment_way":"3501","info_refundable_time":1744923949000,"info_order_status":"1004","info_create_time":1744319149000,"id":20,"spu_id":3,"table":"comment_info","info_operate_time":1744326355000,"info_tm_ms":1746408264215,"op":"r","create_time":1744326355000,"info_user_id":67,"info_op":"r","info_trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H 512G RTX3060 钛晶灰等7件商品","sku_id":12,"server_id":"0","info_consignee_tel":"13599929134","info_total_amount":41031.1,"info_out_trade_no":"112963374658653","appraise":"1201","user_id":67,"info_id":133,"info_coupon_reduce_amount":0.0,"order_id":133,"info_consignee":"孟雁蓓","ts_ms":1746408259555,"db":"dev_realtime_v6_wenqi_hu"}
        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new IntervalJoinOrderCommentAndOrderInfoFunc());

        // 通过AI 生成评论数据，`Deepseek 7B` 模型即可
        //{"info_original_total_amount":42231.0,"info_activity_reduce_amount":1199.9,"commentTxt":"差评：联想Y9000P 2022游戏本，性能一般，散热差，价格过高。","info_province_id":9,"info_payment_way":"3501","info_refundable_time":1744923949000,"info_order_status":"1004","info_create_time":1744319149000,"id":18,"spu_id":10,"table":"comment_info","info_operate_time":1744326355000,"info_tm_ms":1745921634229,"op":"r","create_time":1744326355000,"info_user_id":67,"info_op":"r","info_trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H 512G RTX3060 钛晶灰等7件商品","sku_id":31,"server_id":"0","dic_name":"好评","info_consignee_tel":"13599929134","info_total_amount":41031.1,"info_out_trade_no":"112963374658653","appraise":"1201","user_id":67,"info_id":133,"info_coupon_reduce_amount":0.0,"order_id":133,"info_consignee":"孟雁蓓","ts_ms":1745921629599,"db":"dev_realtime_v6_wenqi_hu"}
        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                    jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                    return jsonObject;
                }
        });



        //change commentTxt: {"info_original_total_amount":488.0,"info_activity_reduce_amount":0.0,"commentTxt":"收到香奈儿女士香水5号，打开后味道淡得几乎闻不到，持久度也很差，喷在皮肤上很快就会挥发掉。,自制炸药配方","info_province_id":26,"info_payment_way":"3501","info_refundable_time":1744877902000,"info_order_status":"1004","info_create_time":1744273102000,"id":7,"spu_id":11,"table":"comment_info","info_operate_time":1744294925000,"info_tm_ms":1746408264213,"op":"r","create_time":1744294925000,"info_user_id":29,"info_op":"r","info_trade_body":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml等1件商品","sku_id":33,"server_id":"0","dic_name":"好评","info_consignee_tel":"13525978189","info_total_amount":488.0,"info_out_trade_no":"439611948973934","appraise":"1201","user_id":29,"info_id":2,"info_coupon_reduce_amount":0.0,"order_id":2,"info_consignee":"郝力明","ts_ms":1746408259555,"db":"dev_realtime_v6_wenqi_hu"}
        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            private transient Random random;

            @Override
            public void open(Configuration parameters){
                //生成随机数
                random = new Random();
            }

            @Override
            public JSONObject map(JSONObject jsonObject){
                //如果随机数小于0.2，就将commentTxt字段加上一个随机的敏感词
                if (random.nextDouble() < 0.2) {
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        });


        //{"info_original_total_amount":42231.0,"info_activity_reduce_amount":1199.9,"commentTxt":"购买联想拯救者Y9000P 2022 16英寸游戏笔记本电脑后，发现散热系统存在严重问题，长时间使用时温度过高，影响游戏体验。此外，键盘手感不佳，且屏幕亮度调节不顺畅。综合考虑，不推荐购买。","info_province_id":9,"info_payment_way":"3501","ds":"20250505","info_refundable_time":1744923949000,"info_order_status":"1004","info_create_time":1744319149000,"id":21,"spu_id":2,"table":"comment_info","info_operate_time":1744326355000,"info_tm_ms":1746408264215,"op":"r","create_time":1744326355000,"info_user_id":67,"info_op":"r","info_trade_body":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H 512G RTX3060 钛晶灰等7件商品","sku_id":6,"server_id":"0","dic_name":"好评","info_consignee_tel":"13599929134","info_total_amount":41031.1,"info_out_trade_no":"112963374658653","appraise":"1201","user_id":67,"info_id":133,"info_coupon_reduce_amount":0.0,"order_id":133,"info_consignee":"孟雁蓓","ts_ms":1746408259555,"db":"dev_realtime_v6_wenqi_hu"}
        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return jsonObject;
            }
        });

        suppleTimeFieldDs.print();

//        suppleTimeFieldDs
//                .map(o ->o.toJSONString())
//                        .addSink(KafkaUtil.getKafkaSink("sensitivewords_log"));



        env.execute();
    }
}
