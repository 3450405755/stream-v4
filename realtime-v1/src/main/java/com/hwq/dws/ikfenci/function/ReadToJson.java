package com.hwq.dws.ikfenci.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/**
 * @Package com.hwq.dws.ikfenci.function
 * @Author com.hwq
 * @Date 2025/5/14 14:30
 * @description:
 */
public class ReadToJson {
    public static void main(String[] args) throws IOException {
        // 定义要读取的文件路径
        String filePath = "E:\\工单\\工单\\txt\\timeWeight.txt";
        // 调用readFileToJsonMap方法，将文件内容读取并转换为HashMap，其中键为字符串，值为JSONObject
        HashMap<String, JSONObject> map = readFileToJsonMap(filePath);
        // 打印转换后的HashMap
        System.out.println(map);
        // 遍历HashMap，对每个键值对进行操作
        // 这里的操作是获取JSONObject中键为"40-49"的值并打印
        map.forEach((k, v) -> {
            System.out.println(v.getString("40-49"));
        });



//        JSONObject object = JSON.parseObject(filePath);
//        System.out.println(object);
    }
    public static HashMap<String,JSONObject> readFileToJsonMap(String filePath)  {

        // 创建一个HashMap，用于存储从文件中读取并转换后的数据，键为字符串，值为JSONObject
        HashMap<String, JSONObject> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            // 逐行读取文件内容，直到文件末尾
            while ((line = reader.readLine()) != null) {
                // 将读取到的每行字符串解析为JSONObject对象
                JSONObject object = JSON.parseObject(line);
                // 遍历JSONObject中的所有键
                for (String s : object.keySet()) {
                    // 获取键s对应的值（字符串形式）
                    String values = object.getString(s);
                    // 将获取到的字符串值解析为JSONObject对象
                    JSONObject objects = JSON.parseObject(values);
                    // 将键s和对应的JSONObject对象存入map中
                    map.put(s, objects);
                }
            }
        } catch (IOException e) {
            // 捕获读取文件过程中可能出现的IO异常，并打印异常堆栈信息
            e.printStackTrace();
        }
        // 返回存储了文件转换后数据的HashMap
        return map;
    }
}
