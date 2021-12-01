package com.cjbdi.ws.udfs;

import com.alibaba.fastjson.JSON;
import com.cjbdi.ws.bean.WsBean;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Calendar;


/**
 * @Date 2021/11/24 13:54
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */
public class WsSourceJsonToWsBeanFunction extends ProcessFunction<String, WsBean> {
    private final OutputTag<String> outputTag;
    private boolean flag;
    private int startTime;
    private int stopTime;
    private ValueState<Calendar> calendarValueState;
    private ValueStateDescriptor<Calendar> valueStateDescriptor;

    public WsSourceJsonToWsBeanFunction(OutputTag<String> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool globalJobParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        flag = globalJobParameters.getBoolean("execution-time-flag", false);
        startTime = globalJobParameters.getInt("execution-time-start", 0);
        stopTime = globalJobParameters.getInt("execution-time-stop", 24);

//        valueStateDescriptor = new ValueStateDescriptor<>("calendar", Calendar.class);
//        StateTtlConfig.newBuilder(1000L * 60)
//                .setUpdateType
//        calendarValueState = getRuntimeContext().getState(valueStateDescriptor)

    }

    @Override
    public void processElement(String s, ProcessFunction<String, WsBean>.Context context, Collector<WsBean> collector) throws Exception {

        if (flag) {
            int hour;
            Calendar calendar = Calendar.getInstance();
            hour = calendar.get(Calendar.HOUR_OF_DAY);

            while (hour >= 5 && hour < 22) {
                Thread.sleep(1000L * 60);
                calendar.setTimeInMillis(System.currentTimeMillis());
                hour = calendar.get(Calendar.HOUR_OF_DAY);
            }
        }

        try {
            WsBean wsBean = JSON.parseObject(s, WsBean.class);
            collector.collect(wsBean);
        } catch (Exception e) {
            e.printStackTrace();
            context.output(outputTag, s);
        }

    }
}
