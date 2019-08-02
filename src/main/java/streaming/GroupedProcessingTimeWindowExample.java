package streaming;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


import java.util.concurrent.TimeUnit;


/**
 * @Author: xu.dm
 * @Date: 2019/8/2 15:19
 * @Version: 1.0
 * @Description:
 * An example of grouped stream windowing into sliding time windows.
 * This example uses [[RichParallelSourceFunction]] to generate a list of key-value pairs.
 **/
public class GroupedProcessingTimeWindowExample {
    public static void main(String args[]) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Tuple2<Long,Long>> stream =  env.addSource(new DataSource());
        SocketClientSink<Tuple2<Long,Long>> sink = new SocketClientSink<>("192.168.44.10", 9000,
                new SerializationSchema<Tuple2<Long, Long>>() {
                    @Override
                    public byte[] serialize(Tuple2<Long, Long> element) {
                        return (System.currentTimeMillis()+" : "+element.toString()+"\n").getBytes();
                    }
                }
        );
        stream.keyBy(0)
                .timeWindow(Time.of(2500, TimeUnit.MILLISECONDS),Time.of(500, TimeUnit.MILLISECONDS))
                .reduce(new SummingReducer())

                // alternative: use a apply function which does not pre-aggregate
//			.keyBy(new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>())
//                .window(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS))
//			.apply(new SummingWindowFunction())

//                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
//                    @Override
//                    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
//                        System.out.println(value);
//                    }
//                });

        .addSink(sink);

        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long,Long>>{
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            final long startTime = System.currentTimeMillis();
            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;

            while (isRunning && count<numElements){
//                Thread.sleep(100);
                count++;
                ctx.collect(new Tuple2<>(val++,1L));
                if(val>numKeys){
                    val = 1L;
                }
            }

            final long endTime = System.currentTimeMillis();
            System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class FirstFieldKeyExtractor<Type extends Tuple,Key> implements KeySelector<Type,Key>{
        @Override
        @SuppressWarnings("unchecked")
        public Key getKey(Type value) throws Exception {
            return (Key) value.getField(0);
        }
    }

    private static class SummingWindowFunction implements WindowFunction<Tuple2<Long,Long>,Tuple2<Long,Long>,Long,Window>{
        @Override
        public void apply(Long key, Window window, Iterable<Tuple2<Long, Long>> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            long sum = 0L;
            for(Tuple2<Long,Long> value : input){
                sum+=value.f1;
            }
            out.collect(new Tuple2<>(key,sum));
        }
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long,Long>>{
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0,value1.f1+value2.f1);
        }
    }
}
