package controller;/**
 * @program flink
 * @description: stream api demo
 * @author: lichen
 * @create: 2023/08/21 17:40
 */

import model.Event;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @program flink
 * @description: stream api demo 
 * @author: lichen
 * @create: 2023/08/21 17:40 
 */
public class StreamSource {

    public static void main(String[] args) throws Exception {

        // 默认并行度是本地cpu数量
//        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
//
//        LocalStreamEnvironment localEnvironment1 = StreamExecutionEnvironment.createLocalEnvironment(2);
//
//        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 7777);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.socketTextStream("127.0.0.1",7777);

//         设置批处理模式;尽量在命令行设置启动模式 flink run -Dexecution.runtime-model=BATCH
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

//         获取源数据的就是源算子
        DataStreamSource<String> dataStreamSource = env.readTextFile("");
        // 从元素中读取
        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("jack","/home",""));
        env.fromCollection(eventList);

        // kafka 数据源
        Properties properties = new Properties();

        properties.setProperty("bootstrap.server","127.0.0.1:7777");

        properties.setProperty("group.id","consumer-group");

        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> kafkaStreamSource = env.addSource(new FlinkKafkaConsumer<String>("click", new SimpleStringSchema(), properties));

        kafkaStreamSource.print();

        env.execute();




    }
}
