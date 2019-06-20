import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TraceSourceData {
    public static void main(String args[]) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple5<String, Integer, Long, Integer, Integer>> ds =
                env.addSource(new SourceData())
                        .flatMap(new FlatMapFunction<String, Tuple5<String, Integer, Long, Integer, Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) throws Exception {
                                String ss[] = value.split(",");
                                out.collect(Tuple5.of(ss[0], Integer.parseInt(ss[1]), Long.parseLong(ss[2]), Integer.parseInt(ss[3]), Integer.parseInt(ss[4])));
                            }
                        });

        //5秒窗口统计各状态的次数
        DataStream<Tuple2<Integer, Integer>> statusData = ds
                .flatMap(new FlatMapFunction<Tuple5<String, Integer, Long, Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(Tuple5<String, Integer, Long, Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {

                        out.collect(Tuple2.of(value.f3, 1));
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        statusData.print().setParallelism(1);

        //5秒窗口统计响应时间大于50的用户访问次数在整个响应中的占比
        //大于50，小于等于50，所有次数
        DataStream<Tuple3<Integer, Integer, Integer>> greater100UserPer = ds
                .flatMap(new FlatMapFunction<Tuple5<String, Integer, Long, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void flatMap(Tuple5<String, Integer, Long, Integer, Integer> value, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                        if (value.f4 > 50)
                            out.collect(Tuple3.of(1, 0, 1));
                        else
                            out.collect(Tuple3.of(0, 1, 1));
                    }
                })
                .timeWindowAll(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer, Integer> value2) throws Exception {
                        return Tuple3.of(value1.f0 + value2.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> value) throws Exception {
                        Double rate1 = (value.f0.doubleValue() / value.f2.doubleValue()) * 100;
                        Double rate2 = (value.f1.doubleValue() / value.f2.doubleValue()) * 100;

                        return Tuple3.of(rate1.intValue(), rate2.intValue(), 1);
                    }
                });


        greater100UserPer.addSink(new SinkFunction<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void invoke(Tuple3<Integer, Integer, Integer> value, Context context) throws Exception {
                System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(context.timestamp()), ZoneId.systemDefault()) + " " + value);
            }
        });


        env.execute("TraceSourceData");
    }


}
