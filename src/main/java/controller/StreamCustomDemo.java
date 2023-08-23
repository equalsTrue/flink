package controller;/**
 * @program flink
 * @description: 自定义SourceFunction
 * @author: lichen
 * @create: 2023/08/22 12:06
 */

import model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import source.EventSource;

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

        List<Event> eventList = new ArrayList<>();

        eventList.add(new Event("jack","/pro?id=3",null));

        eventList.add(new Event("flink","/pro?id=3",null));

        eventList.add(new Event("david","/pro?id=3",null));

        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(eventList);

        SingleOutputStreamOperator<String> map = eventDataStreamSource.map(new Mapper());

        map.print();

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
