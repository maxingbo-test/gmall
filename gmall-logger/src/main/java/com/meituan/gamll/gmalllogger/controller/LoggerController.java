package com.meituan.gamll.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping(value = "/applog")
    public String applog(@RequestBody String logString){

        log.info(logString);

        JSONObject jsonObject = JSON.parseObject(logString);

        // kafka 日志分流，可 kafka 分，可 springboot 日志分
        if(jsonObject.getString("start")!=null && jsonObject.getString("start").length()>0){
            // 启动日志：topic名字
            // 会自动创建topic
            kafkaTemplate.send("gmall_start",logString);
        } else {
            // 事件日志：topic名字
            kafkaTemplate.send("gmall_event",logString);
        }

        return logString;
    }

}
