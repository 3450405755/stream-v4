package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSONObject;
import com.hwq.until.ConfigUtils;
import com.hwq.until.JdbcUtils;
import com.hwq.bean.DimBaseCategory;
import com.hwq.dws.ikfenci.function.DimCategoryCompare;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.hwq.dws.ikfenci.function
 * @Author com.hwq
 * @Date 2025/5/13 21:34
 * @description: 设备打分模型
 */
public class MapDeviceAndSearchMarkModelFunc extends RichMapFunction<JSONObject,JSONObject> {

    // 设备评分权重系数（控制设备评分在最终结果中的影响程度）
    private final double deviceRate;
    // 搜索内容评分权重系数（控制搜索内容评分在最终结果中的影响程度）
    private final double searchRate;
    // 分类映射表：三级分类名称 -> 分类对象（用于快速查找分类信息）
    private final Map<String, DimBaseCategory> categoryMap;
    // 分类对比数据列表（从数据库加载，用于将一级分类映射到搜索类别）
    private List<DimCategoryCompare> dimCategoryCompares;
    // 数据库连接对象（用于加载分类对比数据）
    private Connection connection;


    /**
     * 构造函数：初始化分类映射表和权重参数
     * @param dimBaseCategories 三级分类维度数据列表（用于构建categoryMap）
     * @param deviceRate 设备评分权重系数
     * @param searchRate 搜索评分权重系数
     */
    public MapDeviceAndSearchMarkModelFunc(List<DimBaseCategory> dimBaseCategories, double deviceRate,double searchRate) {
        this.deviceRate = deviceRate;
        this.searchRate = searchRate;
        this.categoryMap = new HashMap<>();
        // 将三级分类数据存入Map，以三级分类名称作为键，加速后续查询
        for (DimBaseCategory category : dimBaseCategories) {
            categoryMap.put(category.getB3name(), category);
        }
    }


    /**
     * Flink生命周期方法：在函数初始化时执行，用于资源初始化
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 建立MySQL数据库连接（实际生产环境建议使用配置文件管理连接信息）
        connection = JdbcUtils.getMySQLConnection(
                "jdbc:mysql://cdh03:3306",
                "root",
                "root");
        // 查询分类对比数据，将一级分类映射到搜索类别（如"手机" -> "科技与数码"）
        String sql = "select id, category_name, search_category from dev_realtime_v6_wenqi_hu.category_compare_dic;";
        // 执行SQL查询并将结果映射为DimCategoryCompare对象列表
        dimCategoryCompares = JdbcUtils.queryList2(connection, sql, DimCategoryCompare.class, true);
        // 调用父类方法完成初始化
        super.open(parameters);
    }


    /**
     * 核心处理方法：对每条输入数据进行设备和搜索内容评分
     */
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // 提取设备操作系统信息（格式示例："iOS,16.4" 或 "Android,13"）
        String os = jsonObject.getString("os");
        // 按逗号分割字符串，获取主操作系统类型（如"iOS"或"Android"）
        String[] labels = os.split(",");
        String judge_os = labels[0];
        // 将处理后的操作系统类型存入结果对象
        jsonObject.put("judge_os", judge_os);

        // 根据设备类型为不同年龄段分配设备评分（权重×设备类型系数）
        if (judge_os.equals("iOS")) {
            jsonObject.put("device_18_24", round(0.7 * deviceRate));
            jsonObject.put("device_25_29", round(0.6 * deviceRate));
            jsonObject.put("device_30_34", round(0.5 * deviceRate));
            jsonObject.put("device_35_39", round(0.4 * deviceRate));
            jsonObject.put("device_40_49", round(0.3 * deviceRate));
            jsonObject.put("device_50",    round(0.2 * deviceRate));
        } else if (judge_os.equals("Android")) {
            jsonObject.put("device_18_24", round(0.8 * deviceRate));
            jsonObject.put("device_25_29", round(0.7 * deviceRate));
            jsonObject.put("device_30_34", round(0.6 * deviceRate));
            jsonObject.put("device_35_39", round(0.5 * deviceRate));
            jsonObject.put("device_40_49", round(0.4 * deviceRate));
            jsonObject.put("device_50",    round(0.3 * deviceRate));
        }

        // 处理搜索内容，关联分类信息
        String searchItem = jsonObject.getString("search_item");
        if (searchItem != null && !searchItem.isEmpty()) {
            // 通过三级分类名称查找分类对象
            DimBaseCategory category = categoryMap.get(searchItem);
            if (category != null) {
                // 若找到匹配分类，提取一级分类名称存入结果对象
                jsonObject.put("b1_category", category.getB1name());
            }
        }

        // search
        // 将一级分类映射到搜索类别（如"手机" -> "科技与数码"）
        String b1Category = jsonObject.getString("b1_category");
        if (b1Category != null && !b1Category.isEmpty()){
            // 遍历分类对比数据，查找匹配的搜索类别
            for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                if (b1Category.equals(dimCategoryCompare.getCategoryName())){
                    // 找到匹配项，存入搜索类别并终止循环
                    jsonObject.put("searchCategory",dimCategoryCompare.getSearchCategory());
                    break;
                }
            }
        }

        // 处理未匹配到搜索类别的情况，设置默认值
        String searchCategory = jsonObject.getString("searchCategory");
        if (searchCategory == null) {
            searchCategory = "unknown";
        }

        switch (searchCategory) {
            case "时尚与潮流":
                jsonObject.put("search_18_24", round(0.9 * searchRate));
                jsonObject.put("search_25_29", round(0.7 * searchRate));
                jsonObject.put("search_30_34", round(0.5 * searchRate));
                jsonObject.put("search_35_39", round(0.3 * searchRate));
                jsonObject.put("search_40_49", round(0.2 * searchRate));
                jsonObject.put("search_50", round(0.1    * searchRate));
                break;
            case "性价比":
                jsonObject.put("search_18_24", round(0.2 * searchRate));
                jsonObject.put("search_25_29", round(0.4 * searchRate));
                jsonObject.put("search_30_34", round(0.6 * searchRate));
                jsonObject.put("search_35_39", round(0.7 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.8    * searchRate));
                break;
            case "健康与养生":
            case "家庭与育儿":
                jsonObject.put("search_18_24", round(0.1 * searchRate));
                jsonObject.put("search_25_29", round(0.2 * searchRate));
                jsonObject.put("search_30_34", round(0.4 * searchRate));
                jsonObject.put("search_35_39", round(0.6 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.7    * searchRate));
                break;
            case "科技与数码":
                jsonObject.put("search_18_24", round(0.8 * searchRate));
                jsonObject.put("search_25_29", round(0.6 * searchRate));
                jsonObject.put("search_30_34", round(0.4 * searchRate));
                jsonObject.put("search_35_39", round(0.3 * searchRate));
                jsonObject.put("search_40_49", round(0.2 * searchRate));
                jsonObject.put("search_50", round(0.1    * searchRate));
                break;
            case "学习与发展":
                jsonObject.put("search_18_24", round(0.4 * searchRate));
                jsonObject.put("search_25_29", round(0.5 * searchRate));
                jsonObject.put("search_30_34", round(0.6 * searchRate));
                jsonObject.put("search_35_39", round(0.7 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.7    * searchRate));
                break;
            default:
                jsonObject.put("search_18_24", 0);
                jsonObject.put("search_25_29", 0);
                jsonObject.put("search_30_34", 0);
                jsonObject.put("search_35_39", 0);
                jsonObject.put("search_40_49", 0);
                jsonObject.put("search_50", 0);
        }


        return jsonObject;

    }

    /**
     * 辅助方法：对数值进行四舍五入（保留3位小数）
     */
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }



    /**
     * Flink生命周期方法：在函数关闭时执行，用于资源释放
     */
    @Override
    public void close() throws Exception {
        // 关闭数据库连接，防止资源泄漏
        super.close();
        connection.close();
    }
}
