package com.hwq.fuction;

import com.alibaba.fastjson.JSONObject;
import org.checkerframework.common.reflection.qual.GetClass;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Package com.hwq.utils.SensitiveWordsUtils
 * @Author com.hwq
 * @Date 2025/5/7
 * @description: sensitive words
 */
public class SensitiveWordsUtils {

    //从指定文件路径读取敏感词,存放到list集合
    public static ArrayList<String> getSensitiveWordsLists(){
        ArrayList<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("E:\\idea实训\\stream-v4\\realtime-common\\src\\main\\java\\com\\hwq\\until\\words.txt"))){
            String line ;
            while ((line = reader.readLine()) != null){
                res.add(line);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        //返回包含所有敏感词
        return res;
    }

    //从给定的泛型列表中随机返回一个元素
    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size());
        return list.get(randomIndex);
    }

    //调用上述两个方法，输出一个随机敏感词。
    public static void main(String[] args) {
        System.err.println(getRandomElement(getSensitiveWordsLists()));
    }
}
