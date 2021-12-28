package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * 跳出明细的实现
 * 跳出率=跳出数=进入数
 *
 * dwm的作用 : 过滤出来所有跳出明细
 * 跳出明细日志的特点:
 *      是数据的入口,这条数据的特征是   last_page_id  是空
 *      后面没有跟着其他日志   一个时间单位(看自身需求设置时间)内没有其他入口
 *
 *
 * 数据来源于页面日志:
 *      从一堆数据中,找到具体特殊特征的数据------------使用CEP技术,使用模式进行筛选,
 *      这里考虑一种特殊情况,即进入页面后快速跳出,然后在单位时间内又再次进入,
 *      其数据如下:
 *          "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
 *          "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
 *          两者都没有下一个页面,但是因为时间间隔较短,所以在模式上很容易把两个当作一个进行处理
 * */
public class DwmUserJumpDetailApp_1 extends BaseAppV1 {
    public static void main(String[] args) {
        new DwmUserJumpDetailApp_1().init(3002, 1, "DwmUserJumpDetailApp_1", "DwmUserJumpDetailApp_1", Constant.TOPIC_DWD_PAGE);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {



        ////测试数据
        //stream =
        //        env.fromElements(
        //                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
        //                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
        //                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
        //                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
        //                        "\"home\"},\"ts\":14000} ",
        //                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
        //                        "\"detail\"},\"ts\":50000} "
        //        );




        KeyedStream<JSONObject, String> keyedStream = stream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((page, ts) -> page.getLong("ts"))
                )
                .keyBy(page -> page.getJSONObject("common").getString("mid"));

        //写CEP找到跳出数据
        // 1. 定义模式
        // a. 入口
        // b:紧跟着一个其他页面,即非入口    last_page_id:有值
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry")
                // 入口
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("normal")
                //非入口
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .within(Time.seconds(5));

        // 2. 将模式作用在流上
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);

        // 3. 从模式流获取匹配到的数据(即获取超时数据)
        SingleOutputStreamOperator<JSONObject> normal = ps.select(
                new OutputTag<JSONObject>("uj") {
                },
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map,
                                              long timeoutTimestamp) throws Exception {
                        return map.get("entry").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String,
                            List<JSONObject>> map) throws Exception {
                        return map.get("entry").get(0);
                    }
                }
        );

        normal
                .union(normal.getSideOutput(new OutputTag<JSONObject>("uj"){}))
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UJ));
                ;

    }
}