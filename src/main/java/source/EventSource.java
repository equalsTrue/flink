package source;/**
 * @program flink
 * @description: 自定义source
 * @author: lichen
 * @create: 2023/08/22 12:16
 */

import model.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @program flink
 * @description: 自定义source
 * @author: lichen
 * @create: 2023/08/22 12:16 
 */
public class EventSource implements SourceFunction<Event> {

    public boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] names = {"jack","david","flink"};
        String[] urls = {"/prod?id=3","/click?id=4","/pay?id=5"};

        while (running){
            String name = names[random.nextInt(names.length)];
            String url = urls[random.nextInt(urls.length)];
            String time = new Timestamp(System.currentTimeMillis()).toString();
            sourceContext.collect(new Event(name,url,time));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
