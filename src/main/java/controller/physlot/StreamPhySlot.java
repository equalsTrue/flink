package controller.physlot;/**
 * @program flink
 * @description: 物理分区
 * @author: lichen
 * @create: 2023/08/28 11:25
 */

import model.Event;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import source.EventSource;

import java.util.ArrayList;
import java.util.List;

/**
 * @program flink
 * @description: 物理分区
 * @author: lichen
 * @create: 2023/08/28 11:25 
 */
public class StreamPhySlot {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
// 测试数据
        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("mary","/pro?id=1",null));
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

        // shuffle 随机打乱分区
        dataStream.map(data -> data.name).returns(Types.STRING).shuffle().print().setParallelism(4);

        // rebalance 平衡分区
        dataStream.map(data -> data.name).returns(Types.STRING).rebalance().print().setParallelism(4);

        // rescale 重缩放分区，将并行数据datasource 先分区，在后续的子任务根据datasource 数据分slot 处理
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                // 将源数据按照奇偶性分成两个分区， 0 slot, 1 slot
                for(int i = 1; i<= 12 ;i ++){
                    if(i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                        sourceContext.collect(i);
                    }

                }
            }

            @Override
            public void cancel() {

            }
            // 输出slot(1,2) 输出偶数； 输出slot(3,4) 输出奇数
        }).setParallelism(2).rescale().print().setParallelism(4);

        // 广播输出， 设置几个并行度，就有几个并行slot 得到数据
        dataStream.broadcast().print("广播：").setParallelism(4);

        // 全局分区，将所有的数据都汇总到一个slot 上，通常用于处理完数据后合并数据，之后设置的并行度也不起作用
        dataStream.global().print("全局").setParallelism(4);

        // 自定义分区, partition 设置分区策略，若分区的数量小于后续子任务的并行度，则按照自定义的分区策略执行
        env.fromElements(1,2,3,4,5,6,7,8,0,10).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int num) {
                if(key == 1){
                    return 0;
                }else if(key == 2){
                    return 1;
                }else {
                    return 2;
                }
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(6);


        env.execute();

    }
}
