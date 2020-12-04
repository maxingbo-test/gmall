package com.meituan.gamll.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @RequestMapping(value = "/applog")
    public String applog(@RequestBody String logString){

        // 日志清洗....123a

        // 日志输出
        log.info("info:{}-{}",logString,"1");
        log.debug("debug:{}",logString);
        return logString;
    }

}
