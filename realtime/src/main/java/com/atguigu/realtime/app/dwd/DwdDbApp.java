package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atgugu.realtime.app.BaseAppV1;
import com.atgugu.realtime.bean.TableProcess;
import com.atgugu.realtime.common.Constant;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class DwdDbApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdDbApp().init(2002, 1, "DwdDbApp", "DwdDbApp", Constant.TOPIC_ODS_DB);
    }
    
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 对stream中的数据进行etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        
        // 2. 读取配置表的数据
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        
        // 3. 把数据流和配置流进行connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream = connect(etledStream, tpStream);
        // 4.动态分流
        
        // 5. 不同的流写入到不同sink中
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream,
                                                                                 SingleOutputStreamOperator<TableProcess> tpStream) {
        /*
        广播状态如何存储, 方便数据流中的数据找到自己的配置信息?
        来一条业务数据, 就去广播状态中找到自己的配置信息
        
         key: 表名:操作类型   "order_info:insert"
         value: TableProcess
         */
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        // 广播状态
        
        // 1. 把配置流做成广播流
        BroadcastStream<TableProcess> tpBCStream = tpStream.broadcast(tpStateDesc);
        // 2. 数据流和广播进行connect
       return dataStream
            .connect(tpBCStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                // 3. 使用广播状态把配置数据广播出去
                // 处理数据流中的信息: 从广播状态读取到当前数据的配置信息, 然后组成一个Tuple2 输出
                @Override
                public void processElement(JSONObject value,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    String key = value.getString("table") + ":" + value.getString("type");
                    TableProcess tp = ctx.getBroadcastState(tpStateDesc).get(key);
                    // 向外输出数据的时候, 只输出了 data, 其他的一些元数据就省略.
                    if (tp != null) {
                        out.collect(Tuple2.of(value.getJSONObject("data"), tp));
                    }
                }
                
                // 处理配置信息: 把配置信息存入到广播状态
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    String key = tp.getSource_table() + ":" + tp.getOperate_type();
                    BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                    tpState.put(key, tp);
                    
                }
            });
        
    }
    
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("create table tp(" +
                            "  `source_table` string," +
                            "  `operate_type` string," +
                            "  `sink_type` string," +
                            "  `sink_table` string," +
                            "  `sink_columns` string," +
                            "  `sink_pk` string," +
                            "  `sink_extend` string," +
                            "  PRIMARY KEY (`source_table`,`operate_type`)not enforced" +
                            ")with(" +
                            "   'connector' = 'mysql-cdc'," +
                            "   'hostname' = 'hadoop162'," +
                            "   'port' = '3306'," +
                            "   'username' = 'root'," +
                            "   'password' = 'aaaaaa'," +
                            "   'database-name' = 'gmall2021_realtime'," +
                            "   'table-name' = 'table_process', " +
                            "   'debezium.snapshot.mode' = 'initial'" +  // 程序一启动, 会先读取表中所有的数据, 然后再根据bin_log去获取变化的数据
                            ")");
        
        Table tp = tEnv.from("tp");
        //將tp表的數據格式封裝成TableProcess類
        return tEnv
            .toRetractStream(tp, TableProcess.class)
            .filter(t -> t.f0)
            .map(t -> t.f1);
        
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        
        return stream
            .map(data -> JSON.parseObject(data.replaceAll("bootstrap-", "")))
            .filter(obj ->
                        obj.getString("database") != null
                            && obj.getString("table") != null
                            && ("insert".equals(obj.getString("type")) || "update".equals(obj.getString("type")))
                            && obj.getString("data") != null
                            && obj.getString("data").length() > 2
            );
    }
}
