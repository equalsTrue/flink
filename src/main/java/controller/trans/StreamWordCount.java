package controller.trans;/**
 * @program flink
 * @description: stream word count
 * @author: lichen
 * @create: 2023/08/18 15:29
 */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @program flink
 * @description: stream word count 
 * @author: lichen
 * @create: 2023/08/18 15:29 
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 创建流式启动环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 全局禁用算子链
        // env.disableOperatorChaining();

        //读取文件
        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/lichen/IdeaProjects/flink/src/main/input/WordCount.txt");

        // flatMap 转化计算, setParallelism(2) 设置并行度 ； disableChaining() 禁用算子链；startNewChain() 开启算子链; slotSharingGroup 设置slot共享组，默认是default
        SingleOutputStreamOperator<Tuple2<String, Long>> streamWordTuple = dataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word,1L));
            }
//        }).returns(Types.TUPLE(Types.STRING,Types.LONG)).setParallelism(2).disableChaining().startNewChain().slotSharingGroup("1");
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        // 分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = streamWordTuple.keyBy(data -> data.f0);

        // 统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1).setParallelism(2);

        // 打印
        sum.print();

        //启动
        env.execute();
    }
}
