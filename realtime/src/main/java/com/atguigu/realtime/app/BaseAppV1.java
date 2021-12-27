package com.atguigu.realtime.app;

import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public abstract class BaseAppV1 {

    protected abstract void run(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void init(int port, int p, String ck,String groupId, String topic){
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();

        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);

        //开启checkpoint
        //周期
        env.enableCheckpointing(3000);
        //状态后端
        env.setStateBackend(new HashMapStateBackend());
        //索引存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/ck/"+ck);;

        //checkpoint并行数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //checkpoint模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        //checkpoint消费间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        //checkpoint删除后的策略
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //开始读取kafka的topic
        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(groupId, topic));
        //具体业务的执行
        run(env,stream);
        try{
            env.execute(ck);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
