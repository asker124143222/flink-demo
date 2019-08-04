package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @Author: xu.dm
 * @Date: 2019/8/4 18:35
 * @Description:
 */
public class TumblingEventWindowExample {
    public static void main(String args[]) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> socketStream = env.socketTextStream("192.168.31.10",9000);
        DataStream<Tuple2<String,Long>> resultStream = socketStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(String element) {
                        long eventTime = Long.parseLong(element.split(" ")[0]);
                        System.out.println(eventTime);
                        return eventTime;
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String,Long> map(String value) throws Exception {
                        return Tuple2.of(value.split(" ")[1],1L);
                    }
                }).keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0,value1.f1+value2.f1);
                    }
                });
        resultStream.print();

        env.execute();
    }

    public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple2<Long,Long>>{
        private final long maxOutofOrderness = 3L;
        private long currentMaxTimestamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp-maxOutofOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, Long> element, long previousElementTimestamp) {
            long timestamp = element.f0;
            currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp);
            return timestamp;
        }
    }
}
