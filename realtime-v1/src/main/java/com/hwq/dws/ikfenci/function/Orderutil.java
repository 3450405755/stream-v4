package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;

/**
 * @Package com.hwq.dws.ikfenci.function.Orderutil
 * @Author hu.wen.qi
 * @Date 2025/5/14 22:59
 * @description: 1
 */
public class Orderutil  extends RichMapFunction<JSONObject,JSONObject> {

    private static final double c1Rate = 0.3;
    private static final double tmRate = 0.2;
    private static final double timeRate = 0.15;
    private static final double priceRate = 0.1;
    //读取本地文件路径
    private static final String c1Path = "E:\\工单\\工单\\txt\\/c1Weight.txt";
    private static final String pricePath = "E:\\工单\\工单\\txt\\/priceWeight.txt";
    private static final String timePath = "E:\\工单\\工单\\txt/timeWeight.txt";
    private static final String tmPath = "E:\\工单\\工单\\txt/tmWeight.txt";
    private static HashMap<String, JSONObject> c1Map;
    private static HashMap<String, JSONObject> tmMap;
    private static HashMap<String, JSONObject> timeMap;
    private static HashMap<String, JSONObject> priceMap;

    static {
        c1Map = ReadToJson.readFileToJsonMap(c1Path);
        tmMap = ReadToJson.readFileToJsonMap(tmPath);
        timeMap = ReadToJson.readFileToJsonMap(timePath);
        priceMap = ReadToJson.readFileToJsonMap(pricePath);
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        //时间数据
        String timeType = jsonObject.getString("time_type");
        JSONObject timeObj = timeMap.get(timeType);
        jsonObject.put("time_code", timeObj);

        //价格区间
        String priceLevel = jsonObject.getString("price_level");
        JSONObject priceObj = priceMap.get(priceLevel);
        jsonObject.put("price_code", priceObj);

        //类目
        String c1 = jsonObject.getString("c1_name");
        JSONObject c1Obj = c1Map.get(c1);
        jsonObject.put("c1_code", c1Obj);

        //品牌
        String tmName = jsonObject.getString("tm_name");
        JSONObject tmObj = tmMap.get(tmName);
        if (tmObj != null) {
            jsonObject.put("tm_code", tmObj);
        } else {
            jsonObject.put("tm_code", tmMap.get("香奈儿"));
        }


        return jsonObject;
    }
}
