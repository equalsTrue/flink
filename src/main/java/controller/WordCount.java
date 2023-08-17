package controller;/**
 * @program flinkDemo
 * @description: flink 统计
 * @author: lichen
 * @create: 2023/08/17 12:00
 */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @program flinkDemo
 * @description: flink 统计 
 * @author: lichen
 * @create: 2023/08/17 12:00 
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        DataSource<String> lineDataSource = env.readTextFile("/Users/lichen/IdeaProjects/flinkDemo/src/main/input/WordCount.txt");

        // 将每行数据进行分词，转成二元组类型
        FlatMapOperator<String,Tuple2<String,Long>> wordTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String,Long>> out) ->{
            // 将每行文本进行拆分
            String[] words = line.split(" ");
            // 将每个单词转成二元组输出
            for(String word :words){
                out.collect(Tuple2.of(word,1L));
            }
            //  java lambda 存在泛型擦除,将指定泛型
        } ).returns(Types.TUPLE(Types.STRING,Types.LONG));

        // 按照word进行分组, 按照Tuple 二元组的数组顺序选择key
        UnsortedGrouping<Tuple2<String, Long>> wordTupleGroup = wordTuple.groupBy(0);

        // 按照key 去求和,也是按照Tuple 二元组的顺序去sum
        AggregateOperator<Tuple2<String, Long>> sum = wordTupleGroup.sum(1);

        sum.print();
    }
}
