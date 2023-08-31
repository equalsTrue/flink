package controller;/**
 * @program flink
 * @description: mysql sink
 * @author: lichen
 * @create: 2023/08/31 18:16
 */

import model.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: mysql sink 
 * @author: lichen
 * @create: 2023/08/31 18:16 
 */
public class StreamMysqlSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("mary","/pro?id=1",null));
        eventList.add(new Event("flink","/pro?id=2",null));
        eventList.add(new Event("jack","/pro?id=3",null));
        eventList.add(new Event("david","/pro?id=4",null));
        eventList.add(new Event("flink","/pro?id=5",null));
        eventList.add(new Event("david","/pro?id=6",null));
        DataStreamSource<Event> dataSource = env.fromCollection(eventList);
        // jdbc sink
        dataSource.addSink(
                JdbcSink.sink(
                        " INSERT INTO flink_event (name,url) values (?,?)", ((statement, event) ->{
                    statement.setString(1,event.name);
                    statement.setString(2,event.url);
                }), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build())
        );
    }
}
