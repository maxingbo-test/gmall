package com.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.gmallpublisher.service.EsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    EsService esService;
    @Value("${dau.id:dau}")
    private String dauId;
    @Value("${dau.name:new_dau}")
    private String dauName;

    //访问路径 http://publisher:8070/realtime-total?date=2019-02-01

    @RequestMapping(value = "realtime-total",method = RequestMethod.GET)
    public String getDauTotal(@RequestParam("date") String dt){
        if(StringUtils.isEmpty(dt))
            return null;
        List<Map<String,Object>> rlist = new ArrayList<>();
        Map<String,Object> _dauMap = new HashMap();
        _dauMap.put("id",dauId);
        _dauMap.put("name",dauName);
        Long dauTotal = esService.getDauTotal(dt);
        _dauMap.put("value",dauTotal);
        rlist.add(_dauMap);
        return JSON.toJSONString(rlist);
    }
}
