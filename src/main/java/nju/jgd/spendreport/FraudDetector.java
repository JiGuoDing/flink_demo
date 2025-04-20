package nju.jgd.spendreport;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

/**
 * 欺诈检查类 FraudDetector 是 KeyedProcessFunction 接口的一个实现。
 * 他的方法 KeyedProcessFunction#processElement 将会在每个交易事件上被调用。这个程序里边会对每笔交易发出警报。
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, KeyedProcessFunction<Long, Transaction, Alert>.Context context, Collector<Alert> collector) throws Exception {

        // get the current state for the current key
        Boolean lastTransactionWasSmall =  flagState.value();

        // check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                // output an alert downStream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            // clean up the state
            cleanup(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, Transaction, Alert>.OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

    private void cleanup(Context ctx) throws IOException {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }
}
