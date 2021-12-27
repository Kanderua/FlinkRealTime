package com.atguigu.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class DwdLogApp extends BaseAppV1 {

    private static final String PAGE = "page";
    private static final String DISPLAY = "display";
    private static final String START = "start";

    public static void main(String[] args) {
        //ck是checkpoint,也就是第一个DwdLogApp,而第二个DwdLogApp则是groupId，是Kafka的
        new DwdLogApp().init(2001, 1, "DwdLogApp", "DwdLogApp", Constant.TOPIC_ODS_LOG);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        //对流进行处理
        //stream.print();

        // 1. 纠正新老客户的
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(stream);
        // 2. 分流:页面 曝光 启动
        HashMap<String, DataStream<JSONObject>> threeStreams = splitStream(validatedStream);


        // 3. 不同的流写入不同个topic中
        write2Kafka(threeStreams);
    }

    private void write2Kafka(HashMap<String, DataStream<JSONObject>> threeStreams) {
        threeStreams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_PAGE));
        threeStreams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_DISPLAY));
        threeStreams
                .get(START)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_START));
    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>(PAGE) {        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>(DISPLAY) {        };
        //分流  頁面  曝光  啟動
        SingleOutputStreamOperator<JSONObject> startStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        //啟動(主流)  曝光和頁面(側輸出流)
                        JSONObject start = value.getJSONObject("start");
                        if (start != null) {
                            //啟動日誌
                            out.collect(value);
                        } else {
                            //同一條日誌即有可能是曝光日誌也有可能是頁面日誌所以不再做elseif判斷
                            JSONObject page = value.getJSONObject("page");
                            if (page != null) {
                                //側輸出流
                                ctx.output(pageTag, value);
                            }

                            JSONArray displays = value.getJSONArray("displays");
                            if (displays != null) {
                                //將曝光數據由一條數據包含多個信息列切分成一個信息包含一個完整的信息
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);

                                    display.put("ts", value.getLong("ts"));
                                    display.putAll(value.getJSONObject("common"));
                                    display.putAll(value.getJSONObject("page"));
                                    ctx.output(displayTag, display);

                                }
                            }
                        }
                    }
                });


        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        DataStream<JSONObject> displayStream = startStream.getSideOutput(displayTag);

        // 返回三個流:可以考慮以下三種結構
        // Tuple   list   map
        // 最終選擇map,Tuple和list需要經常查看,不容易記憶;
        HashMap<String, DataStream<JSONObject>> map = new HashMap<>();
        map.put(START, startStream);
        map.put(DISPLAY, displayStream);
        map.put(PAGE, pageStream);
        return map;
    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(DataStreamSource<String> stream) {
        /*
        如何判斷一個用戶是否是新老用戶

        使用狀態

        時間時間 窗口

        第一個窗口中時間戳最小的是is_new: 1
        其他情況都是0

        */

        return stream
                .map(JSON::parseObject)
                //添加水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                //按照數據信息進行分類
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                //流入各自的窗口中
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //對窗口中的數據進行處理
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    private ValueState<Boolean> firstWindowState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("firstWindowState", Boolean.class));
                    }

                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        if (firstWindowState.value() == null) {
                            firstWindowState.update(true);

                            List<JSONObject> list = IterToList.toList(elements);
                            //JSONObject min = Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")));
                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                            //遍歷數據,對數據進行is_new設置
                            for (JSONObject obj : list) {
                                // 判斷對象是否相等
                                if (obj == min) {
                                    obj.getJSONObject("common").put("is_new", "1");
                                } else {
                                    obj.getJSONObject("common").put("is_new", "0");
                                }
                                out.collect(obj);
                            }
                        } else {
                            //不是第一個窗口,則將所有的元素is_new設置為0,表示不是新用戶
                            for (JSONObject obj : elements) {
                                obj.getJSONObject("common").put("is_new", "0");
                                out.collect(obj);
                            }
                        }
                    }
                });
    }
}
