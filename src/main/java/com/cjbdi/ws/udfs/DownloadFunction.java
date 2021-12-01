package com.cjbdi.ws.udfs;

import com.alibaba.fastjson.JSON;
import com.cjbdi.ws.bean.WsBean;
import com.cjbdi.ws.bean.WsBeanDownloaded;
import com.cjbdi.ws.services.WsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Base64;

/**
 * @Date 2021/11/24 14:56
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */

@Slf4j
public class DownloadFunction extends ProcessFunction<WsBean, WsBeanDownloaded> {
    private WsService wsService;
    private final OutputTag<String> outputTag;
    private String url;

    public DownloadFunction(OutputTag<String> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(WsBean wsBean, ProcessFunction<WsBean, WsBeanDownloaded>.Context context, Collector<WsBeanDownloaded> collector) throws Exception {
        try {
//            if (!wsService.isCreated()) {
//                wsService = new WsService(url, 300000);
//            }
            String ws_c_nr = wsBean.getWs_c_nr();
            byte[] wsFile = wsService.getWritContent(ws_c_nr);

//            if (wsFile == null) {
//                for (int i = 0; i < 10; i++) {
//                    if (wsFile == null) {
//                        wsFile = wsService.getWritContent(ws_c_nr);
//                        Thread.sleep(1000 * 10);
//                    }
//                }
//            }

            WsBeanDownloaded wsBeanDownloaded = new WsBeanDownloaded(wsBean, Base64.getEncoder().encodeToString(wsFile));
            collector.collect(wsBeanDownloaded);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(wsBean.toString());
            context.output(outputTag, JSON.toJSONString(wsBean));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        url = parameterTool.getRequired("hyapi-url");
        wsService = new WsService(url, 300000);
    }
}
