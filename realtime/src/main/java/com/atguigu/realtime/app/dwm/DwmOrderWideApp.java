package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;
import static com.atguigu.realtime.common.Constant.TOPIC_DWD_ORDER_DETAIL;
import static com.atguigu.realtime.common.Constant.TOPIC_DWD_ORDER_INFO;
/**
 * 与之关联的表有order_info,order_detail
 * 两张事实表
 *
 * 两者join形成一张宽表
 * 两表可以以流的形式进行join,
 * 流的join使用   interval join      join的结果仍是事实表
 *
 * 俩表join后仍需要进行补充一些维度数据
 * 而维度数据不适合做成流,因为数据变化较慢
 * 因此事实表和维度表的join需要手动join
 * 即在事实表中插入每一条数据时,需要根据维度的id,去获取相应的维度字段
 *
 * */
public class DwmOrderWideApp extends BaseAppV2 {

    public static void main(String[] args) {
        new DwmOrderWideApp().init(3003, 1, "DwmOrderWideApp", "DwmOrderWideApp",
                TOPIC_DWD_ORDER_INFO, TOPIC_DWD_ORDER_DETAIL);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       HashMap<String, DataStreamSource<String>> topicToStream) {
        //1. 事实表的join
        SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDims = factsJoin(topicToStream);
        //2. join维度数据
        SingleOutputStreamOperator<OrderWide> orderWideStreamDims = factDims(orderWideStreamWithoutDims);
        orderWideStreamDims.print();
        //3. 将宽表数据写入kafka中
    }

    private SingleOutputStreamOperator<OrderWide> factDims(SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDims) {
        /*
        * 每来一条数据,都需要通过phoenix中查找对应的维度数据,需要查询六张维度表
        *
         */
        return orderWideStreamWithoutDims.map(new RichMapFunction<OrderWide, OrderWide>() {

            private Connection phoenixConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                //建立jdbc连接对象
                phoenixConn = JdbcUtil.getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);
            }

            @Override
            public void close() throws Exception {
                if(phoenixConn!=null){
                    phoenixConn.close();
                }
            }

            @Override
            public OrderWide map(OrderWide value) throws Exception {
                //执行六个SQL,去查找对应的维度数据
                JSONObject userInfo= DimUtil.readDimFromPhoenix(phoenixConn,"dim_user_info",value.getUser_id());
                value.setUser_gender(userInfo.getString(""));

                return null;
            }
        });
    }

    private SingleOutputStreamOperator<OrderWide> factsJoin(HashMap<String, DataStreamSource<String>> topicToStream) {
        //  interval join 连接两个事实表
        //  注意事项: 其只支持事件时间,且必须是keyBy之后使用

        //order_info事实表的数据流
        KeyedStream<OrderInfo, Long> orderInfoStream = topicToStream
                .get(TOPIC_DWD_ORDER_INFO)
                .map(info -> JSON.parseObject(info, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((info, ts) -> info.getCreate_ts())
                )
                .keyBy(OrderInfo::getId);

        //order_detail事实表的数据流
        KeyedStream<OrderDetail, Long> orderDetailStream = topicToStream
                .get(TOPIC_DWD_ORDER_DETAIL)
                .map(info -> JSON.parseObject(info, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((detail, ts) -> detail.getCreate_ts())
                )
                .keyBy(OrderDetail::getOrder_id);


        //join两张事实表数据流
        return orderInfoStream
                .intervalJoin(orderDetailStream)
                //乱序程度设置区间
                .between(Time.seconds(-5),Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left,
                                               OrderDetail right,
                                               Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left,right));
                    }
                });
    }
}
