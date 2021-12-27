package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;

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

        // 4.过滤掉一些不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filteredStream = filterColumns(dataTpStream);
        //测试
        //filteredStream.print();

        // 5.动态分流   事实表到Kafka, 一个流         到hbase的是另一个流
        Tuple2<DataStream<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> kafkaHbaseStream = dynamicSplit(filteredStream);
        //测试
        //kafkaHbaseStream.f0.print();
        //kafkaHbaseStream.f1.print();

        // 6. 不同的流写入到不同sink中
        writeToKafka(kafkaHbaseStream.f0);
        writeToHbase(kafkaHbaseStream.f1);
    }

    private void writeToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {

    }

    private void writeToKafka(DataStream<Tuple2<JSONObject, TableProcess>> stream) {
        stream.addSink(FlinkSinkUtil.getKafkaSink());
    }

    private Tuple2<DataStream<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicSplit(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {

        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbase"){};

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = stream.process(new ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public void processElement(Tuple2<JSONObject, TableProcess> value,
                                       Context ctx,
                                       Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                //主流去Kafka,侧输出流区hbase

                //读取配置表的划定的那些表,那些是去Kafka那些区hbase,对此进行判断
                String sinkType = value.f1.getSink_type();
                if (Constant.SINK_TO_KAFKA.equals(sinkType)) {
                    out.collect(value);
                } else if (Constant.SINK_TO_HBASE.equals(sinkType)) {
                    ctx.output(hbaseTag, value);
                }
            }
        });

        //封装侧输出流数据
        DataStream<Tuple2<JSONObject, TableProcess>> hbaseStream = kafkaStream.getSideOutput(hbaseTag);

        return Tuple2.of(kafkaStream,hbaseStream);
    }


    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        return stream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> t) throws Exception {
                JSONObject data = t.f0;
                TableProcess tp = t.f1;

                List<String> columns = Arrays.asList(tp.getSink_columns().split(","));
                //JSONObject是一个map,现在需要删除map中的某个键值对,实际上移除key就等于将键值对移除
                data.keySet().removeIf(key -> !columns.contains(key));
                return t;
            }
        });
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
