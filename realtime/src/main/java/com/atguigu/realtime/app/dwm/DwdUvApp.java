package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.IterToList;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 从用户的多次访问记录中获取到第一次的访问记录
 * 让数据进入到同一个窗口中,添加水映,那么同一批数据中,水印最小的值就认为是第一次访问记录
 */
public class DwdUvApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdUvApp().init(3001, 1, "DwdUvApp", "DwdUvApp", Constant.TOPIC_DWD_PAGE);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                //事件时间
                                .withTimestampAssigner((page, ts) -> page.getLong("ts"))
                )
                .keyBy(page -> page.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //对数据进行去重
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    private SimpleDateFormat sdf;
                    private ValueState<String> dateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 时间数据(ts)格式化
                        sdf = new SimpleDateFormat("yyyy-MM-dd");


                        // 存储日期数据    2021-12-28
                        dateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("dateState", String.class));
                    }

                    /**
                     * 获取最小的时间
                     * */
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        //考虑跨天的情况,当跨天时,清空状态dateState
                        String yesterday = dateState.value();
                        String today = sdf.format(ctx.window().getStart());
                        if (!today.equals(yesterday)) {
                            dateState.clear();
                        }


                        if (dateState.value() == null) {
                            List<JSONObject> list = IterToList.toList(elements);
                            //Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")))
                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                            out.collect(min);

                            dateState.update(sdf.format(min.getLong("ts")));
                        }
                    }
                })
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UV));

    }
}
/*
dwd_page dwd_start dwd_display
消费的topic:
选择dwd_start还是dwd_page?

选择启动日志, 计算的uv值会偏小  只有app启动的时候才有启动日志, 如果通过浏览器访问, 则没有启动日志

选择页面日志

 */