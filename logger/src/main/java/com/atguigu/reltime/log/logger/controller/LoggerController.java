package com.atguigu.reltime.log.logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 * @author yj2333
 */
@Controller
@ResponseBody
@Slf4j
public class LoggerController {

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String log){
        //1. 将数据落盘
        saveToDisk(log);

        //2. 把日志数据写入Kafka
        writeToKafka(log);

        return "ok";
    }

    @Autowired
    KafkaTemplate<String,String> kafka;
    private void writeToKafka(String log) {
        kafka.send("ods_log",log);
    }

    private void saveToDisk(String data) {
        //调用了@Slf4j注解，可以直接使用log函数
        //将文件写入指定路径，路径由数据制造的配置文件决定
        log.info(data);
    }
}
