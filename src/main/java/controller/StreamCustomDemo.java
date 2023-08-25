package controller;/**
 * @program flink
 * @description: 自定义SourceFunction
 * @author: lichen
 * @create: 2023/08/22 12:06
 */

import model.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: 自定义SourceFunction
 * @author: lichen
 * @create: 2023/08/22 12:06 
 */
public class StreamCustomDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

//        DataStreamSource<Event> eventDataStreamSource = env.addSource(new EventSource());

        // 设置自定义并行度sourceFunction
//        DataStreamSource<Object> dataStreamSource = env.addSource(new ParallelSource()).setParallelism(2);

        // 测试数据
        List<Event> eventList = new ArrayList<>();

        eventList.add(new Event("jack","/pro?id=1",null));

        eventList.add(new Event("jack","/pro?id=1",null));


        eventList.add(new Event("flink","/pro?id=2",null));

        eventList.add(new Event("david","/pro?id=3",null));

        eventList.add(new Event("jack","/pro?id=1",null));


        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(eventList);

        // map 转化

//        SingleOutputStreamOperator<String> map = eventDataStreamSource.map(new Mapper());

        SingleOutputStreamOperator<String> map = eventDataStreamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.name;
            }
        });

//        lambda
//        SingleOutputStreamOperator<String> map = eventDataStreamSource.map(data -> data.name);

//        map.print();

        // filter 过滤
        SingleOutputStreamOperator<Event> filter = eventDataStreamSource.filter(data -> data.name.equals("jack"));

        SingleOutputStreamOperator<String> map1 = filter.map(data -> data.name);


        //flatmap lambda (先Map 再打散数据， flink 可以一对多)
        eventDataStreamSource.flatMap((Event event, Collector<String> out) -> {
            // collect 负责收集数据，多次输出
            out.collect(event.url);
            out.collect(event.name);
        }).returns(new TypeHint<String>() {
        });

        // flatMap 匿名内部类
        SingleOutputStreamOperator<String> flatStream = eventDataStreamSource.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                collector.collect(event.name);
                collector.collect(event.url);
            }
        }).returns(Types.STRING);

        // keyby 分组 （相当于key 的分区，同一个key 放同一个slot 处理，相当于MapReduce中的map）
        SingleOutputStreamOperator<Event> eventStringKeyedStream =
                eventDataStreamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event s) throws Exception {
                return s.name;
            }
        }).max("name");


        SingleOutputStreamOperator<Tuple2<String, Long>> returns = eventDataStreamSource.flatMap((Event event, Collector<Tuple2<String, Long>> out) -> {
            out.collect(Tuple2.of(event.name, 1L));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = returns.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).sum(1);


        sum.print();

        env.execute();

    }


    private static class ParallelSource implements ParallelSourceFunction {
        @Override
        public void run(SourceContext sourceContext) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }

    private static class Mapper implements MapFunction<Event,String> {

        @Override
        public String map(Event event) throws Exception {
            return event.name;
        }
    }
}
