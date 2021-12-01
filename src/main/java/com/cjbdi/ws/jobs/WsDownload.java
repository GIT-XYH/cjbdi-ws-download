package com.cjbdi.ws.jobs;

import com.cjbdi.ws.bean.WsBean;
import com.cjbdi.ws.bean.WsBeanDownloaded;
import com.cjbdi.ws.udfs.Base64ToFileProcessFunction;
import com.cjbdi.ws.udfs.DownloadFunction;
import com.cjbdi.ws.udfs.WsBeanDownloadedToJsonProcessFunction;
import com.cjbdi.ws.udfs.WsSourceJsonToWsBeanFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;

/**
 * @Date 2021/11/24 13:22
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */
public class WsDownload {
    public static void main(String[] args) throws Exception {
        //创建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取全局参数
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        //传递全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        //开启checkpoint
        env.enableCheckpointing(1000L * 60, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());

        //配置checkpoint参数
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(parameterTool.getRequired("checkpoint-dir"));
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        checkpointConfig.setMinPauseBetweenCheckpoints(1000L * 30);


        //设置
        checkpointConfig.setCheckpointTimeout(Integer.MAX_VALUE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10L)));

        //配置并创建kafkaSource
        String brokers = parameterTool.getRequired("bootstrap-servers");
        String inputTopic = parameterTool.getRequired("input-topic");
        String groupId = parameterTool.getRequired("input-group-id");
        String jsonErrorTopic = parameterTool.getRequired("json-error-topic");
        String downloadErrorTopic = parameterTool.getRequired("download-error-topic");
        String outputTopic = parameterTool.getRequired("output-topic");
        String writeErrorTopic = parameterTool.getRequired("write-error-topic");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "ws-source");


        OutputTag<String> jsonErrorData = new OutputTag<String>("json-error-data") {};
        //json -> wsBean
        SingleOutputStreamOperator<WsBean> wsBeanStream = kafkaStream.process(new WsSourceJsonToWsBeanFunction(jsonErrorData));

        //输出json解析失败数据
        DataStream<String> jsonErrorStream = wsBeanStream.getSideOutput(jsonErrorData);
        KafkaSink<String> jsonErrorSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(jsonErrorTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        jsonErrorStream.sinkTo(jsonErrorSink);


        OutputTag<String> downloadError = new OutputTag<String>("jobs-error") {};
        //下载文书,wsBean -> wsBeanDownloaded
        SingleOutputStreamOperator<WsBeanDownloaded> downloadedStream = wsBeanStream.process(new DownloadFunction(downloadError));

        //输出请求api失败数据
        DataStream<String> downloadErrorStream = downloadedStream.getSideOutput(downloadError);
        KafkaSink<String> downloadErrorSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(downloadErrorTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        downloadErrorStream.sinkTo(downloadErrorSink);

        Properties properties = new Properties();
        properties.setProperty("max.request.size", "214748364");
        properties.setProperty("compression.type", "gzip");
        properties.setProperty("buffer.memory", "335544320");
        properties.setProperty("batch.size", "1638400");
        properties.setProperty("max.block.ms", "214748364");

        //输出最终结果
        SingleOutputStreamOperator<String> outputStream = downloadedStream.process(new WsBeanDownloadedToJsonProcessFunction());
        KafkaSink<String> outputSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        outputStream.sinkTo(outputSink);


        OutputTag<String> writeErrorData = new OutputTag<String>("write-error-data") {};

        SingleOutputStreamOperator<String> errorData = downloadedStream.process(new Base64ToFileProcessFunction(writeErrorData));

        DataStream<String> errorDataSideOutput = errorData.getSideOutput(writeErrorData);



        //输出错误数据
        KafkaSink<String> writeErrorSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(writeErrorTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        errorDataSideOutput.sinkTo(writeErrorSink);

        //执行
        JobExecutionResult execute = env.execute("ws-jobs");

    }
}
