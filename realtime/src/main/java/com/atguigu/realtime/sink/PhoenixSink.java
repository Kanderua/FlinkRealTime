package com.atguigu.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.JdbcUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection phoenixConn;
    private ValueState<Boolean> tableCreatedSteate;


    @Override
    public void open(Configuration parameters) throws Exception {
        //open方法的执行次数应该和并行度一致
        //连接phoenix需要建立连接
        phoenixConn = JdbcUtil.getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);

        tableCreatedSteate = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("tableCreate", Boolean.class));
    }

    @Override
    public void invoke(Tuple2<JSONObject,
                       TableProcess> value,
                       Context ctx) throws Exception {
        // 流中每来一个元素就执行一次,建立连接需要使用open
        //实现建表和插入数据的sql语句

        // 1.建表
        checkTable(value);
        // 2. 把数据写入表中
        writeToPhoenix(value);

    }

    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;

        // 执行插入语句
        // upsert into user(name, id)values(?,?)
        StringBuilder sql = new StringBuilder();
        // 拼接sql语句 TODO
        sql
                .append("upsert into ")
                .append(tp.getSink_table())
                .append("(")
                .append(tp.getSink_columns())
                .append(")values(")
                .append(tp.getSink_columns().replaceAll("[^,]+", "?"))
                .append(")");

        PreparedStatement ps = phoenixConn.prepareStatement(sql.toString());
        // 给ps设置占位符 TODO
        String[] columnNames = tp.getSink_columns().split(",");
        for (int i = 0; i < columnNames.length; i++) {
            Object v = data.get(columnNames[i]);
            ps.setString(i + 1, v == null ? null : v.toString());  // null -> "null"
        }
        ps.execute();
        phoenixConn.commit();
        ps.close();

    }

    private void checkTable(Tuple2<JSONObject,TableProcess> value) throws SQLException, IOException {
        if (tableCreatedSteate.value() == null) {
            TableProcess tp = value.f1;
            // 建表
            // 使用标准的sql进行建表操作
            // 1. 拼接SQL语句,此处在phoenix建表时全部使用varchar类型,由于预先无法知道数据类型,而varchar可以较好的兼容所有的数据类型
            //  sql :  create table if not exists user(id varchar , name varchar , constraint pk primary key(id,name))
            StringBuilder sql = new StringBuilder();
            sql
                    .append("create table if not exists ")
                    .append(tp.getSink_table())
                    .append("(")
                    .append(tp.getSink_columns().replaceAll("([^,]+)", "$1 varchar"))
                    .append(", constraint pk primary key(")
                    .append(tp.getSink_pk() == null ? "id" : tp.getSink_pk())
                    .append("))")
                    .append(tp.getSink_extend() == null ? "" : tp.getSink_extend());

            System.out.println("建表语句:" + sql.toString());

            // 2. 执行sql语句
            PreparedStatement ps = phoenixConn.prepareStatement(sql.toString());
            // 2.1 如果sql中由占位符,给占位符赋值,此处是建表语句,没有所谓的占位符
            // 2.2 执行
            ps.execute();
            phoenixConn.commit();
            ps.close();

            tableCreatedSteate.update(true);
        }

    }

    @Override
    public void close() throws Exception {
        if (phoenixConn != null) {
            phoenixConn.close();
        }
    }
}
