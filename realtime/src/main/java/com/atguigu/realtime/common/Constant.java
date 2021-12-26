package com.atguigu.realtime.common;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/12/25 9:13
 */
public class Constant {
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
    
    //ods层的topic
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";
    
    //dwd层topic
    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_DISPLAY = "dwd_display";
    public static final String TOPIC_DWD_START = "dwd_start";
}