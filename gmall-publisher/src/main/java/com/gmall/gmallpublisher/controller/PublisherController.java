package com.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.gmallpublisher.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    // 访问路径 http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
    @RequestMapping(value = "realtime-hour",method = RequestMethod.GET)
    public String getDauHour(@RequestParam("date") String dt
                            ,@RequestParam("id") String id){
        Map dauHourMapTD = esService.getDauHour(dt);
        Map dauHourMapYD = esService.getDauHour(getYdDate(dt));
        Map<String,Map<String,Long>> _map = new HashMap<>();
        _map.put("yesterday",dauHourMapYD);
        _map.put("today",dauHourMapTD);
        return JSON.toJSONString(_map);
    }
    // 昨天
    private String getYdDate(String date){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date dt = dateFormat.parse(date);
            Date ydday = DateUtils.addDays(dt, -1);
            return dateFormat.format(ydday);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("时间格式异常");
        }
    }
}
