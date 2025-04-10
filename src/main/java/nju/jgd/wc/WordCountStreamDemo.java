package nju.jgd.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountStreamDemo {
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(WordCountStreamDemo.class);
    public static void main(String[] args) throws Exception {
        logger.info("Starting WordCountStreamDemo...");

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读取数据
        DataStreamSource<String> lineDS = env.readTextFile("src/main/resources/data.txt");
        // 3. 处理数据，切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    // 转换为二元组并通过采集器向下游发送数据
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 分组：KeySelector<输入数据，分组的Key>
        KeyedStream<Tuple2<String, Integer>, String> wordOneKS = wordOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> wordOne) throws Exception {
                // 定义如何从数据中提取出 Key
                return wordOne.f0;
            }
        });
        // 聚合：
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordOneKS.sum(1);
        // 4. 输出数据
        sumDS.print();
        // 5. 执行，类似 SparkStreaming 的 ssc.start()
        env.execute();
    }
}
