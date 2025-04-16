package nju.jgd.spendreport;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * 欺诈检查类 FraudDetector 是 KeyedProcessFunction 接口的一个实现。
 * 他的方法 KeyedProcessFunction#processElement 将会在每个交易事件上被调用。这个程序里边会对每笔交易发出警报。
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(Transaction transaction, KeyedProcessFunction<Long, Transaction, Alert>.Context context, Collector<Alert> collector) throws Exception {
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
