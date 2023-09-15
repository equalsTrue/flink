package controller.sink;/**
 * @program flink
 * @description: 输出
 * @author: lichen
 * @create: 2023/08/29 17:01
 */

import model.Event;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @program flink
 * @description: 文件输出
 * @author: lichen
 * @create: 2023/08/29 17:01 
 */
public class StreamFileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
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
        DataStreamSource<Event> dataSource = env.fromCollection(eventList);
        // stream 分布式文件写入 ，将文件拆分到bucket写入， forRowFormat 文件按行写入， sinkFunction
        StreamingFileSink<String> streamingFileSink = StreamingFileSink
                .<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("utf-8"))
                // 滚动策略
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        // 每个文件最大，当大于时重新写新文件
                        .withMaxPartSize(1024 * 1024 * 1024)
                        // 多长时间生成一个新的文件
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        // 当多长时间没有新数据进入时写新文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)
                        ).build())
                .build();

        // 将source 转化后添加到sink function中
        dataSource.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();



    }

}
