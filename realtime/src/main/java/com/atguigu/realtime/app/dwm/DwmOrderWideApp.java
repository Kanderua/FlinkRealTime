package com.atguigu.realtime.app.dwm;

import com.atguigu.realtime.app.BaseAppV2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        topicToStream.get(TOPIC_DWD_ORDER_INFO).print("info");
        topicToStream.get(TOPIC_DWD_ORDER_DETAIL).print("detail");
    }
}
