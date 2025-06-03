/**
 * @Package com.hwq.Interface
 * @Author hu.wen.qi
 * @Date 2025/6/3 16:37
 * @description: 从API获取数据并处理为JSON格式，写入CSV文件
 */
package com.hwq.test;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVWriter;

public class Interface {
    private static final String API_URL = "https://opendata.sz.gov.cn/api/29200_00403627/1/service.xhtml?page=1&rows=10&appKey=5c64794593b04d4ebfebb08863339359";

    public static void main(String[] args) {
        try {
            // 发送HTTP请求并获取响应
            String jsonResponse = fetchDataFromApi(API_URL);

            // 解析JSON数据
            ObjectMapper objectMapper = new ObjectMapper();
            // 配置JSON格式化输出
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
            Map<String, Object> responseMap = objectMapper.readValue(jsonResponse, Map.class);

            // 打印JSON格式结果
            System.out.println("API响应JSON:");
            System.out.println(objectMapper.writeValueAsString(responseMap));

            // 处理解析后的数据
            System.out.println("\n总记录数: " + responseMap.get("total"));

            // 处理数据列表并写入CSV
            if (responseMap.containsKey("data")) {
                List<Map<String, Object>> rows = (List<Map<String, Object>>) responseMap.get("data");
                System.out.println("\n数据行数: " + rows.size());

                // 写入CSV文件（确保中文正常显示）
                writeDataToCsv(rows, "data.csv");
                System.out.println("\n数据已成功写入 data.csv");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String fetchDataFromApi(String apiUrl) throws IOException {
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        // 设置请求方法和超时时间
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);

        // 处理响应
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                return response.toString();
            }
        } else {
            throw new IOException("HTTP请求失败，状态码: " + responseCode);
        }
    }

    private static void writeDataToCsv(List<Map<String, Object>> dataList, String filePath) throws IOException {
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(
                new FileOutputStream(filePath), StandardCharsets.UTF_8))) {

            // 添加BOM头，确保Excel正确显示UTF-8编码
            writer.writeNext(new String[]{"\uFEFF"}, false);

            // 写入表头
            if (!dataList.isEmpty()) {
                Map<String, Object> firstRow = dataList.get(0);
                String[] headers = firstRow.keySet().toArray(new String[0]);
                writer.writeNext(headers);

                // 写入每一行数据，处理特殊字符和嵌套对象
                for (Map<String, Object> row : dataList) {
                    String[] csvRow = new String[headers.length];
                    for (int i = 0; i < headers.length; i++) {
                        Object value = row.get(headers[i]);
                        csvRow[i] = formatCsvValue(value);
                    }
                    writer.writeNext(csvRow);
                }
            }
        }
    }

    private static String formatCsvValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof Map || value instanceof List) {
            try {
                // 将嵌套对象转换为JSON字符串
                return new ObjectMapper().writeValueAsString(value);
            } catch (Exception e) {
                return value.toString();
            }
        }
        // 处理普通值，添加引号避免特殊字符问题
        String strValue = value.toString();
        if (strValue.contains(",") || strValue.contains("\"") || strValue.contains("\n")) {
            // 用双引号包裹并转义内部的双引号
            strValue = "\"" + strValue.replace("\"", "\"\"") + "\"";
        }
        return strValue;
    }
}
