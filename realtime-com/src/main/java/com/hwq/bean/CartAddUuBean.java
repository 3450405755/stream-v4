package com.hwq.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author hu.wen.qi
 * @Date 2025/5/4
 * 用户独立访问数
 * */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
