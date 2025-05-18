package com.hwq.gongDan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hwq.until.DateFormatUtil;
import com.hwq.until.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Package com.hwq.gongDan.UserToOrder
 * @Author hu.wen.qi
 * @Date 2025/5/15 10:04
 * @description: 1
 */
public class UserToOrder {

    private static final String[] list2 = {"18_24", "25_29", "30_34", "35_39", "40_49", "50"};

    private static final String[] list4 = {"18-24", "25-29", "30-34", "35-39", "40-49", "50"};
    private static final double c1Rate = 0.3;
    private static final double tmRate = 0.2;
    private static final double timeRate = 0.15;
    private static final double priceRate = 0.1;
    private static final List<String> rank = new ArrayList<>();
    static {
        rank.add("18-24");
        rank.add("25-29");
        rank.add("30-34");
        rank.add("35-39");
        rank.add("40-49");
        rank.add("50以上");

    }
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取用户
        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "win2_minutes", "a2");
        SingleOutputStreamOperator<JSONObject> user = kafkaSource.map(JSON::parseObject);
        //读取订单
        DataStreamSource<String> kafkaSource1 = KafkaUtil.getKafkaSource(env, "order_two", "a2");
        SingleOutputStreamOperator<JSONObject> order = kafkaSource1.map(JSON::parseObject);
//        order.print();

        KeyedStream<JSONObject, String> user_key = user.filter(jsonObject -> !jsonObject.getString("uid").isEmpty() && jsonObject.containsKey("uid"))
                .keyBy(jsonObject -> jsonObject.getString("uid"));

        KeyedStream<JSONObject, String> order_key = order.filter(jsonObject -> !jsonObject.getString("uid").isEmpty() && jsonObject.containsKey("uid"))
                .keyBy(jsonObject -> jsonObject.getString("uid"));


        SingleOutputStreamOperator<JSONObject> user_details_information = user_key
                .intervalJoin(order_key)
                .between(Time.days(-10), Time.days(10))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                        System.err.println(left);
//                        System.err.println(right);
                        left.put("uid", right.getString("uid"));
                        left.put("tm_name", right.getString("tm_name"));
                        left.put("time_type", right.getString("time_type"));
                        left.put("create_time", right.getString("create_time"));
                        left.put("split_total_amount", right.getString("split_total_amount"));
                        left.put("price", right.getString("price"));
                        JSONObject tm_code = right.getJSONObject("tm_code");
                        JSONObject time_code = right.getJSONObject("time_code");
                        JSONObject amountWeight = new JSONObject();
                        for (String s : list4) {
                            double v = right.getDoubleValue("amount_" + s);
                            String s1 = s.replace("-", "_");
                            amountWeight.put(s1, v);
                        };


                        JSONObject search = new JSONObject();
                        for (String s : list4) {
                            double v = left.getDoubleValue("search_" + s);
                            String s1 = s.replace("-", "_");
                            search.put(s1, v);
                        };

                        JSONObject device = new JSONObject();
                        for (String s : list4) {
                            double v = left.getDoubleValue("device_" + s);
                            String s1 = s.replace("-", "_");
                            device.put(s1, v);
                        };

                        JSONObject keyword = new JSONObject();
                        for (String s : list4) {
                            double v = right.getDoubleValue("keyword_" + s);
                            String s1 = s.replace("-", "_");
                            keyword.put(s1, v);
                        };

                        String age = getInferredAge(search,  tm_code, time_code, amountWeight, device, keyword);

                       left.put("inferredAge", age);
                        out.collect(left);
                    }
                });

      user_details_information.print("--------------------------------------------");

        //user_details_information.writeAsText("E:\\001浏览器/output.csv").setParallelism(1);


        env.disableOperatorChaining();
        env.execute();
    }

    public static String getInferredAge(JSONObject c1 ,JSONObject tm,JSONObject time ,JSONObject price,JSONObject device,JSONObject keyword) {

        ArrayList<Double> rankCods = new ArrayList<Double>();
        for (String s : rank) {

            try{
                double v = c1.getDouble(s) * c1Rate
                        + tm.getDouble(s) * tmRate
                        + time.getDouble(s) * timeRate
                        + price.getDouble(s) * priceRate
                        + device.getDouble(s)
                        + keyword.getDouble(s) ;
                rankCods.add(v);
            }catch (Exception e){

//判断是否有空值（前面type可能错误）
//                System.err.println("time= "+time);
//                System.err.println("c= "+c1);
//                System.err.println("t= "+tm);
//                System.err.println("p= "+price);
//                System.err.println("d= "+device);
//                System.err.println("k= "+keyword);
            }
        }
        rankCods.add(0.0);

        double maxValue2 = Collections.max(rankCods);

        int index2 = rankCods.indexOf(maxValue2);

        return rank.get(index2);

    }
}
