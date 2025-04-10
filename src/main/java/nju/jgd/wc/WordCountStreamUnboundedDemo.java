package nju.jgd.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountStreamUnboundedDemo {
    private static final Logger log = LoggerFactory.getLogger(WordCountStreamUnboundedDemo.class);
    public static void main(String[] args) throws Exception {
        log.info("Starting WordCountStreamUnboundedDemo...");
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 从 socket 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("mapreduce", 35678);
        // 3. 处理数据
        // 当 lambda 表达式有泛型时，需要显示指定类型 .returns(Types.{type1}, Types.{type2},...)
        socketDS.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((Tuple2<String, Integer> wordOne) -> wordOne.f0).sum(1).print();
        // 4. 输出
        // 5. 执行
        env.execute();
    }
}
