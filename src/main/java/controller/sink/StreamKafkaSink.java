package controller.sink;/**
 * @program flink
 * @description: flink kafka sink
 * @author: lichen
 * @create: 2023/08/30 12:15
 */

import model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @program flink
 * @description: flink kafka sink 
 * @author: lichen
 * @create: 2023/08/30 12:15 
 */
public class StreamKafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // kafka 数据源
        Properties properties = new Properties();

        properties.setProperty("bootstrap.server","127.0.0.1:7777");

        properties.setProperty("group.id","consumer-group");

        // 创建kafka source
        DataStreamSource<String> kafkaStreamSource = env.addSource(new FlinkKafkaConsumer<String>
                ("click", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> map = kafkaStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] params = value.split(",");
                return new Event(params[0], params[1], params[2]).toString();
            }
        });
        map.addSink(new FlinkKafkaProducer<String>("127.0.0.1:7777","events",new SimpleStringSchema()));

        env.execute();

    }
}
