package com.atguigu.realtime.util;


import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;



import java.util.Properties;

/**
 * @author yj233333
 */
public class FlinkSourceUtil {
    public static SourceFunction<String> getKafkaSource(String groupId, String topic){
        //存储配置信息的类
        Properties props = new Properties();
        //配置信息设置
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("group.id",groupId);
        props.put("auto.offset.reset","latest");
        props.put("isolation.level","read_committed");

        return new FlinkKafkaConsumer<String>(
                topic,new SimpleStringSchema(),
                props
        );
    }
}
