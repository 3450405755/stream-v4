package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.hwq.dws.ikfenci.function.FlinkWorkOrderFuc
 * @Author hu.wen.qi
 * @Date 2025/5/13 14:38
 * @description: 1
 */
public class FlinkWorkOrderFuc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject result = new JSONObject();
        if (left.containsKey("common") && left.getJSONObject("common") != null) {
            JSONObject common = left.getJSONObject("common");
            result.put("uid", common.getString("uid"));
            result.put("ts",right.getString("ts"));
            common.remove("sid");
            common.remove("mid");
            common.remove("is_new");
            result.put("common", common);
            out.collect(result);
        }
    }
}
