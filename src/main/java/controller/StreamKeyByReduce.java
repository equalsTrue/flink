package controller;/**
 * @program flink
 * @description: stream keyby reduce
 * @author: lichen
 * @create: 2023/08/25 18:50
 */

import model.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: stream keyby reduce 
 * @author: lichen
 * @create: 2023/08/25 18:50 
 */
public class StreamKeyByReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 测试时设置并行度为1，在同一slot 上处理，如果并行度大于1，数据将分散在不同的slot上，数据流的顺序将不固定.
        env.setParallelism(1);
        // 测试数据
        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("jack","/pro?id=1",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("jack","/pro?id=1",null));
        eventList.add(new Event("david","/pro?id=3",null));
        eventList.add(new Event("flink","/pro?id=1",null));
        eventList.add(new Event("david","/pro?id=3",null));
        eventList.add(new Event("david","/pro?id=3",null));
        eventList.add(new Event("david","/pro?id=3",null));
        DataStreamSource<Event> dataStream = env.fromCollection(eventList);
        // 先Map映射为(name,1L) 再keyby 分组 然后统计数量 (sum),
        SingleOutputStreamOperator<Tuple2<String, Long>> clickCount = dataStream.map(data -> Tuple2.of(data.name, 1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });
        // 然后取出Count最大的元素
        SingleOutputStreamOperator<Tuple2<String, Long>> returns = clickCount.keyBy(data -> "user").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        returns.print("max: ");

        env.execute();


    }
}
