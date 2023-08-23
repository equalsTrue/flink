package model;/**
 * @program flink
 * @description: event model
 * @author: lichen
 * @create: 2023/08/21 19:10
 */

/**
 * @program flink
 * @description: event model 
 * @author: lichen
 * @create: 2023/08/21 19:10 
 */
public class Event {

    public String name;

    public String url;

    public String time;

    public Event() {
    }

    public Event(String name, String url, String time) {
        this.name = name;
        this.url = url;
        this.time = time;
    }


    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}


