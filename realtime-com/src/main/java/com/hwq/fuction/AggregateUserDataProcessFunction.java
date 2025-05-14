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
 * @Package com.hwq.dws.function
 * @Author com.hwq
 * @Date 2025/5/13 16:28
 * @description:
 */
public class AggregateUserDataProcessFunction extends KeyedProcessFunction<String, JSONObject,JSONObject> {

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
        String os = jsonObject.getString("os");        // 操作系统
        String ch = jsonObject.getString("ch");        // 渠道
        String md = jsonObject.getString("md");        // 设备型号
        String ba = jsonObject.getString("ba");        // 浏览器
        String keyWord = jsonObject.containsKey("keyword") ? jsonObject.getString("keyword") : null;  // 搜索关键词

        // 更新各种字段的集合（去重存储）
        updateField("os", os);
        updateField("ch", ch);
        updateField("md", md);
        updateField("ba", ba);
        if (keyWord != null) {
            updateField("keyword", keyWord);  // 仅当关键词不为空时更新
        }

        // 构建输出结果
        JSONObject output = new JSONObject();
        output.put("uid", jsonObject.getString("uid"));  // 用户ID
        output.put("pv", currentPV);                     // 当前PV值

        // 将各种字段的集合转换为逗号分隔的字符串
        output.put("os", String.join(",", getField("os")));
        output.put("ch", String.join(",", getField("ch")));
        output.put("md", String.join(",", getField("md")));
        output.put("ba", String.join(",", getField("ba")));
        output.put("keyword", String.join(",", getField("keyword")));

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
