package controller.watermark;/**
 * @program flink
 * @description: watermark
 * @author: lichen
 * @create: 2023/09/15 18:31
 */

import controller.util.DateUtil;
import model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: watermark
 * @author: lichen
 * @create: 2023/09/15 18:31 
 */
public class StreamWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(100);
        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("jack","/pro?id=1",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("jack","/pro?id=1",null));
        eventList.add(new Event("david","/pro?id=3",null));
        eventList.add(new Event("david","/pro?id=3",null));
        eventList.add(new Event("flink","/pro?id=1",null));
        eventList.add(new Event("david","/pro?id=3",null));
        eventList.add(new Event("harry","/pro?id=3",null));
        eventList.add(new Event("david","/pro?id=3",null));
        // 正向递增时间 生成水位线，时间是递增的
        env.fromCollection(eventList)
                // forMonotonousTimestamps 正向生成水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        // 水位线时间提取器
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long time) {
                        return DateUtil.parseDayByDayFormat(event.time);
                    }
                }));
        //乱序时间 水位线生成策略
        env.fromCollection(eventList)
                // forBoundedOutOfOrderness 乱序生成水位线,Duration.ofSeconds(2) ，延迟时间为2秒
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                 //  水位线时间提取器
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return DateUtil.parseDayByDayFormat(event.time);
                    }
                }));
    }

}
