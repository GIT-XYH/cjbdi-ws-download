package com.cjbdi.ws.udfs;

import com.alibaba.fastjson.JSON;
import com.cjbdi.ws.bean.WsBeanDownloaded;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Date 2021/11/24 15:42
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */
public class WsBeanDownloadedToJsonProcessFunction extends ProcessFunction<WsBeanDownloaded, String> {
    @Override
    public void processElement(WsBeanDownloaded wsBeanDownloaded, ProcessFunction<WsBeanDownloaded, String>.Context context, Collector<String> collector) throws Exception {
        try {
            String s = JSON.toJSONString(wsBeanDownloaded);
            collector.collect(s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
