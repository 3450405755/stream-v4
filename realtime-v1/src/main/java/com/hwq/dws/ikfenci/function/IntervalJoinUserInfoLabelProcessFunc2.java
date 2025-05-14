package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.hwq.dws.ikfenci.function.IntervalJoinUserInfoLabelProcessFunc2
 * @Author hu.wen.qi
 * @Date 2025/5/14 21:41
 * @description: 1
 */
public class IntervalJoinUserInfoLabelProcessFunc2 extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();
        if (left.getString("id").equals(right.getString("id"))){
            result.putAll(left);
            result.put("tm_id",right.getString("tm_id"));
        }
        out.collect(result);
    }
}
