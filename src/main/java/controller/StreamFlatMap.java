package controller;/**
 * @program flink
 * @description: stream flatMap
 * @author: lichen
 * @create: 2023/08/25 18:19
 */

import model.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: stream flatMap 
 * @author: lichen
 * @create: 2023/08/25 18:19 
 */
public class StreamFlatMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 测试数据
        List<Event> eventList = new ArrayList<>();

        eventList.add(new Event("jack","/pro?id=1",null));

        eventList.add(new Event("jack","/pro?id=1",null));


        eventList.add(new Event("flink","/pro?id=2",null));

        eventList.add(new Event("david","/pro?id=3",null));

        eventList.add(new Event("jack","/pro?id=1",null));

        DataStreamSource<Event> streamSource = env.fromCollection(eventList);

        // flatMap 分组 ，keyBy , sum 统计
        streamSource.flatMap((Event event, Collector<Tuple2<String,Long>> out) ->{

            out.collect(Tuple2.of(event.name,1L));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG)).keyBy(data ->data.f0).sum(1).print("统计姓名：");
        env.execute();
    }
}
