package com.hwq.fuction;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
/**
 * @Package com.hwq.fuction.AggregateUserDataProcessFunction1
 * @Author hu.wen.qi
 * @Date 2025/5/14 19:10
 * @description: 1
 */
public class AggregateUserDataProcessFunction1 extends KeyedProcessFunction<String, JSONObject,JSONObject> {

    // 存储状态
    private transient ValueState<Long> pvState;

    // 存储各种字段的集合状态（如操作系统、渠道、设备型号等）
    // 键为字段名，值为该字段出现过的所有不同值的集合
    //使用 MapState 存储各种字段的集合，实现去重统计
    private transient MapState<String, Set<String>> fieldsState;

    /**
     * 初始化函数，在算子启动时调用
     * 负责创建和初始化状态变量
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV状态描述符，默认值为null
        ValueStateDescriptor<Long> pvDescriptor = new ValueStateDescriptor<>("pv-state", Long.class);
        pvState = getRuntimeContext().getState(pvDescriptor);

        // 初始化字段集合状态描述符
        // 使用TypeHint保留泛型信息，确保状态序列化正确
        MapStateDescriptor<String, Set<String>> fieldsDescriptor = new MapStateDescriptor<>(
                "fields-state",                // 状态名称
                Types.STRING,                  // 键类型：字符串
                TypeInformation.of(new TypeHint<Set<String>>() {})  // 值类型：字符串集合
        );
        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor);
    }

    /**
     * 处理每个输入元素的核心函数
     * jsonObject: 输入的JSON对象
     * ctx: 上下文对象，可获取时间戳、定时器等
     * out: 输出收集器，用于发送处理后的数据
     */
    @Override
    public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
        // 更新PV计数
        Long currentPV = pvState.value();
        if (currentPV == null) {
            currentPV = 1L;  // 首次访问，PV为1
        } else {
            currentPV++;     // 非首次访问，PV加1
        }
        pvState.update(currentPV);

        // 从JSON对象中提取设备和用户行为信息
        String birthday = jsonObject.getString("birthday");
        String decade = jsonObject.getString("decade");
        String gender = jsonObject.getString("gender");
        String zodiac_sign = jsonObject.getString("zodiac_sign");
        String phone_num = jsonObject.getString("phone_num");
        String email = jsonObject.getString("email");
        String uname = jsonObject.getString("uname");
        String login_name = jsonObject.getString("login_name");
        String user_level = jsonObject.getString("user_level");
        String ts_ms = jsonObject.getString("ts_ms");
        String age = jsonObject.getString("age");
        String os = jsonObject.getString("os");        // 操作系统
        String ch = jsonObject.getString("ch");        // 渠道
        String md = jsonObject.getString("md");        // 设备型号
        String ba = jsonObject.getString("ba");        // 浏览器
        String keyWord = jsonObject.containsKey("keyword") ? jsonObject.getString("keyword") : null;  // 搜索关键词
        String device_35_39 = jsonObject.getString("device_35_39");
        String device_50 = jsonObject.getString("device_50");
        String device_30_34 = jsonObject.getString("device_30_34");
        String device_18_24 = jsonObject.getString("device_18_24");
        String device_25_29 = jsonObject.getString("device_25_29");
        String device_40_49 = jsonObject.getString("device_40_49");
        String search_25_29 = jsonObject.getString("search_25_29");
        String search_50 = jsonObject.getString("search_50");
        String search_40_49 = jsonObject.getString("search_40_49");
        String search_18_24 = jsonObject.getString("search_18_24");
        String search_35_39 = jsonObject.getString("search_35_39");
        String search_30_34 = jsonObject.getString("search_30_34");


        // 更新各种字段的集合（去重存储）
        updateField("birthday", birthday);
        updateField("decade", decade);
        updateField("gender", gender);
        updateField("zodiac_sign", zodiac_sign);
        updateField("phone_num", phone_num);
        updateField("email", email);
        updateField("uname", uname);
        updateField("login_name", login_name);
        updateField("user_level", user_level);
        updateField("ts_ms", ts_ms);
        updateField("age", age);
        updateField("os", os);
        updateField("ch", ch);
        updateField("md", md);
        updateField("ba", ba);
        updateField("keyword", keyWord);
        updateField("device_35_39",device_35_39);
        updateField("device_50",device_50);
        updateField("device_30_34",device_30_34);
        updateField("device_18_24",device_18_24);
        updateField("device_25_29",device_25_29);
        updateField("device_40_49",device_40_49);
        updateField("search_25_29",search_25_29);
        updateField("search_50",search_50);
        updateField("search_40_49",search_40_49);
        updateField("search_18_24",search_18_24);
        updateField("search_35_39",search_35_39);
        updateField("search_30_34",search_30_34);
        if (keyWord != null) {
            updateField("keyword", keyWord);  // 仅当关键词不为空时更新
        }

        // 构建输出结果
        JSONObject output = new JSONObject();
        output.put("uid", jsonObject.getString("uid"));  // 用户ID
        output.put("pv", currentPV);                     // 当前PV值

        // 将各种字段的集合转换为逗号分隔的字符串
        output.put("birthday", String.join(",", getField("birthday")));
        output.put("decade", String.join(",", getField("decade")));
        output.put("gender", String.join(",", getField("gender")));
        output.put("zodiac_sign", String.join(",", getField("zodiac_sign")));
        output.put("phone_num", String.join(",", getField("phone_num")));
        output.put("email", String.join(",", getField("email")));
        output.put("uname", String.join(",", getField("uname")));
        output.put("login_name", String.join(",", getField("login_name")));
        output.put("user_level", String.join(",", getField("user_level")));
        output.put("ts_ms", String.join(",", getField("ts_ms")));
        output.put("age", String.join(",", getField("age")));
        output.put("os", String.join(",", getField("os")));
        output.put("ch", String.join(",", getField("ch")));
        output.put("md", String.join(",", getField("md")));
        output.put("ba", String.join(",", getField("ba")));
        output.put("keyword", String.join(",", getField("keyword")));
        output.put("device_35_39", String.join(",", getField("device_35_39")));
        output.put("device_50", String.join(",", getField("device_50")));
        output.put("device_30_34", String.join(",", getField("device_30_34")));
        output.put("device_18_24", String.join(",", getField("device_18_24")));
        output.put("device_25_29", String.join(",", getField("device_25_29")));
        output.put("device_40_49", String.join(",", getField("device_40_49")));
        output.put("search_25_29", String.join(",", getField("search_25_29")));
        output.put("search_50", String.join(",", getField("search_50")));
        output.put("search_40_49", String.join(",", getField("search_40_49")));
        output.put("search_18_24", String.join(",", getField("search_18_24")));
        output.put("search_35_39", String.join(",", getField("search_35_39")));
        output.put("search_30_34", String.join(",", getField("search_30_34")));

        // 将结果发送到下游
        out.collect(output);
    }

    /**
     * 更新指定字段的集合
     * field: 字段名（如"os", "ch"等）
     * value: 字段值
     */
    private void updateField(String field, String value) throws Exception {
        // 获取当前字段对应的集合
        Set<String> set = fieldsState.get(field);
        if (set == null) {
            set = new HashSet<>();  // 如果集合不存在，创建新集合
        }
        set.add(value);             // 添加新值（自动去重）
        fieldsState.put(field, set);  // 更新状态
    }

    /**
     * 获取指定字段的集合
     * field: 字段名
     * 返回: 对应的集合，如果不存在则返回空集合
     */
    private Set<String> getField(String field) throws Exception {
        Set<String> set = fieldsState.get(field);
        return set == null ? Collections.emptySet() : set;
    }
}
