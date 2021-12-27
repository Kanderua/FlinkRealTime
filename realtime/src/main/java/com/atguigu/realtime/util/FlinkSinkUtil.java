package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSinkUtil {
    public static SinkFunction<String> getKafkaSink(String topic) {
        //使用嚴格一次的語義
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        //事務超時時間
        props.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<String>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                    @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static SinkFunction<Tuple2<JSONObject,TableProcess>> getKafkaSink(){
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        //事務超時時間
        props.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element,
                                                                    @Nullable Long timestamp) {
                        String topic = element.f1.getSink_table();
                        byte[] data = element.f0.toJSONString().getBytes(StandardCharsets.UTF_8);
                        return new ProducerRecord<>(topic,data);
                    }
                },
                //配置文件
                props,
                //消费语义
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
}
