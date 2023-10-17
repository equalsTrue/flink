package controller.trans;/**
 * @program flink
 * @description: stream keyby reduce
 * @author: lichen
 * @create: 2023/08/25 18:50
 */

import model.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * @program flink
 * @description: stream keyby reduce 
 * @author: lichen
 * @create: 2023/08/25 18:50 
 */
public class StreamTransFormPro {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 测试时设置并行度为1，在同一slot 上处理，如果并行度大于1，数据将分散在不同的slot上，数据流的顺序将不固定.
        env.setParallelism(1);
        // 测试数据
        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("jack","/pro?id=1",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("jack","/pro?id=1",null));
        eventList.add(new Event("david","/pro?id=3",null));
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
        // 分组max 时，不能用name 做分组条件，否则判断不同的key 会分散到不同的slot中，不能比价大小,max 比较大的是大于或等于就是默认最大的
        SingleOutputStreamOperator<Tuple2<String, Long>> maxOp = clickCount.keyBy(data -> "key").maxBy(1);
        maxOp.print("max 2: ");

        // 然后取出Count最大的元素，将所有二元数组全部放在同一个分区中，然后比较他们统计的个数，如果后面的数比之前的大就返回后面的数，如果相等也返回后面的数
        clickCount.keyBy(data -> "user").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG)).print("reduce inner class max: ");
        // reduce lambda
       clickCount.keyBy(data -> "name").reduce((s1,s2) ->{
            if(s1.f1 > s2.f1){
                return s1;
            }else {
                return s2;
            }
       }).print("reduce lambda max: ");


       // 自定义富函数，可以设置算子的状态，声明周期

        SingleOutputStreamOperator<String> richOp = dataStream.keyBy(data -> data.name).map(new RichMapFunction<Event, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 获取运行时上下文
                getRuntimeContext().getTaskName();

            }


            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public String map(Event event) throws Exception {
                return event.name;
            }
        });


        env.execute();


    }
}
