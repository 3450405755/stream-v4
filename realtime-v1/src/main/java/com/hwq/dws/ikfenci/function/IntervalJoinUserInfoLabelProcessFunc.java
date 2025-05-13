package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc
 * @Author hu.wen.qi
 * @Date 2025/5/13 10:45
 * @description: 1
 */
public class IntervalJoinUserInfoLabelProcessFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();
        if (left.getString("uid").equals(right.getString("uid"))){
            result.putAll(left);
            result.put("height",right.getString("height"));
            result.put("unit_height",right.getString("unit_height"));
            result.put("weight",right.getString("weight"));
            result.put("unit_weight",right.getString("unit_weight"));
        }
        out.collect(result);
    }














}
