package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc1
 * @Author hu.wen.qi
 * @Date 2025/5/14 21:08
 * @description: 1
 */
public class IntervalJoinUserInfoLabelProcessFunc1 extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();
        if (left.getString("id").equals(right.getString("id"))){
            result.putAll(left);
            result.put("sku_id",right.getString("sku_id"));
            result.put("split_total_amount",right.getDouble("split_total_amount"));
        }
        out.collect(result);
    }
}
