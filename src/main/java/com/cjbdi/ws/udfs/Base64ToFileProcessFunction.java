package com.cjbdi.ws.udfs;

import com.alibaba.fastjson.JSON;
import com.cjbdi.ws.bean.WsBean;
import com.cjbdi.ws.bean.WsBeanDownloaded;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

/**
 * @Date 2021/11/26 14:20
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */
public class Base64ToFileProcessFunction extends ProcessFunction<WsBeanDownloaded, String> {
    private OutputTag<String> writeErrorData;
    private String fileDir;
    private SimpleDateFormat simpleDateFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool globalJobParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        fileDir = globalJobParameters.get("file-dir");
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    }

    public Base64ToFileProcessFunction(OutputTag<String> outputTag) {
        this.writeErrorData = outputTag;
    }


    @Override
    public void processElement(WsBeanDownloaded wsBeanDownloaded, ProcessFunction<WsBeanDownloaded, String>.Context context, Collector<String> collector) throws Exception {

        try {
//            WsBeanDownloaded wsBeanDownloaded = JSON.parseObject(s, WsBeanDownloaded.class);
            WsBean wsBean = wsBeanDownloaded.getWsBean();
            String base64File = wsBeanDownloaded.getBase64File();
            byte[] file = Base64.getDecoder().decode(base64File);



            String path = fileDir + wsBean.getHost() + "/" +
                    wsBean.getDatabase() + "/" +
                    wsBean.getSchema() + "/" +
                    simpleDateFormat.format(new Date()) + "/";



            String fileName = wsBean.getFbId() + "&" +
                    wsBean.getSchema() + "&" +
                    wsBean.getT_c_stm() + "&" +
                    wsBean.getT_c_baah() + "&" +
                    wsBean.getWs_c_nr().replaceAll("/", "#");


            File parent = new File(path);
            if (!parent.exists()) {
                parent.mkdirs();
            }

            File target = new File(parent, fileName);
            if (target.exists()) {
                target.delete();
            }
            FileUtils.writeByteArrayToFile(target, file);

        } catch (Exception e) {
            e.printStackTrace();
            context.output(writeErrorData, JSON.toJSONString(wsBeanDownloaded));
        }
    }
}
