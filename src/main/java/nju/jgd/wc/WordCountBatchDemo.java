package nju.jgd.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {



    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读取数据
        DataSource<String> lineDS = env.readTextFile("src/main/resources/data.txt");
        // 3. 切分、转换（word, 1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                /*
                    s 是输入的元素， collector 是采集器
                 */
                // 按空格切分出单词
                String[] words = s.split(" ");
                for (String word : words) {
                    // 3.1 将单词转换为（word, 1）
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // 3.2 使用 collector 向下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });
        // 4. 按照 word 分组，0 是索引，代表 Tuple 中的第一个元素
        UnsortedGrouping<Tuple2<String, Integer>> wordOneGroup = wordOne.groupBy(0);
        // 5. 按分组聚合，1 是索引，代表 Tuple 中的第二个元素
        AggregateOperator<Tuple2<String, Integer>> wordCount = wordOneGroup.sum(1);
        // 6. 输出
        wordCount.print();
    }
}
