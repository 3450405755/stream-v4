package com.hwq.fuction;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * @Package com.hwq.fuction
 * @Author com.hwq
 * @Date
 * @description: 对完整数据进行去重
 */
public class ProcessFilterRepeatTsData extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessFilterRepeatTsData.class);
    // 用于存储已处理数据的状态变量，使用HashSet确保唯一性
    private ValueState<HashSet<String>> processedDataState;

    /**
     * 初始化函数，在算子启动时调用
     * 负责创建和初始化状态变量
     */
    @Override
    public void open(Configuration parameters) {
        // 创建状态描述符
        // "processedDataState" 是状态名称
        // TypeInformation指定状态的数据类型为HashSet<String>
        ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                "processedDataState",
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<HashSet<String>>() {})
        );

        // 从运行时上下文中获取状态对象
        processedDataState = getRuntimeContext().getState(descriptor);
    }

    /**
     * 处理每个输入元素的核心函数
     * value: 输入的JSON对象
     * ctx: 上下文对象，可获取时间戳、定时器等
     * out: 输出收集器，用于发送处理后的数据
     */
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 从状态中获取已处理数据的集合
        HashSet<String> processedData = processedDataState.value();

        // 如果状态为空，创建一个新的HashSet
        if (processedData == null) {
            processedData = new HashSet<>();
        }

        // 将JSON对象转换为字符串表示
        String dataStr = value.toJSONString();

        // 记录处理日志
        LOG.info("Processing data: {}", dataStr);

        // 检查当前数据是否已处理过
        if (!processedData.contains(dataStr)) {
            // 记录新增数据日志
            LOG.info("Adding new data to set: {}", dataStr);

            // 将新数据添加到集合
            processedData.add(dataStr);

            // 更新状态
            processedDataState.update(processedData);

            // 将数据输出到下游
            out.collect(value);
        } else {
            // 记录重复数据日志
            LOG.info("Duplicate data found: {}", dataStr);
        }
    }
}