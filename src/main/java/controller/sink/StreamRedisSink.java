package controller.sink;/**
 * @program flink
 * @description: stream redis sink
 * @author: lichen
 * @create: 2023/08/30 16:46
 */

import model.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: stream redis sink 
 * @author: lichen
 * @create: 2023/08/30 16:46 
 */
public class StreamRedisSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("mary","/pro?id=1",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("jack","/pro?id=3",null));
        eventList.add(new Event("david","/pro?id=4",null));
        eventList.add(new Event("flink","/pro?id=5",null));
        eventList.add(new Event("david","/pro?id=6",null));
        DataStreamSource<Event> dataSource = env.fromCollection(eventList);

        // 创建Redis 连接
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .setDatabase(0)
                .build();

        // 添加redis sink
        dataSource.addSink(new RedisSink<>(config, new RedisMapper<Event>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"flink_redis");
            }

            @Override
            public String getKeyFromData(Event event) {
                return event.name;
            }

            @Override
            public String getValueFromData(Event event) {
                return event.url;
            }
        }));

        env.execute();
    }
}
