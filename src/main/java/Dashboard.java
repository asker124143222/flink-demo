import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @Author: xu.dm
 * @Date: 2019/6/18 21:25
 * @Description: * side output 、table、sql、多sink的使用
 * 1.source从流接收数据，从主流数据中引出side outout进行计算，消费同一个流，避免多次接入，消耗网略
 * 2.通过table和sql计算pv,平均响应时间,错误率（status不等于200的占比）
 *
 * 4.side output的结果数据发送到csv sink
 */
public class Dashboard {
    public static void main(String args[]) throws Exception {
        // traceid,userid,timestamp,status,response time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple5<String, Integer, Long, Integer, Integer>> ds = env.addSource(new SourceData())
                .flatMap(new FlatMapFunction<String, Tuple5<String, Integer, Long, Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) throws Exception {
                        String ss[] = value.split(",");
                        out.collect(Tuple5.of(ss[0], Integer.parseInt(ss[1]), Long.parseLong(ss[2]), Integer.parseInt(ss[3]), Integer.parseInt(ss[4])));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.INT, Types.INT))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple5<String, Integer, Long, Integer, Integer>>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple5<String, Integer, Long, Integer, Integer> lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.f2);
                    }

                    @Override
                    public long extractTimestamp(Tuple5<String, Integer, Long, Integer, Integer> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                });

        final OutputTag<Tuple5<String, Integer, Long, Integer, Integer>> outputTag =
                new OutputTag<Tuple5<String, Integer, Long, Integer, Integer>>("side-output") {
                };

        SingleOutputStreamOperator<Tuple5<String, Integer, Long, Integer, Integer>> mainDataStream = ds
                .process(new ProcessFunction<Tuple5<String, Integer, Long, Integer, Integer>, Tuple5<String, Integer, Long, Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple5<String, Integer, Long, Integer, Integer> value, Context ctx,
                                               Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) throws Exception {
                        ctx.output(outputTag, value);
                        out.collect(value);
                    }
                });

        DataStream<Tuple3<Integer, Integer, Integer>> statusStream = ds
                .flatMap(new FlatMapFunction<Tuple5<String, Integer, Long, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void flatMap(Tuple5<String, Integer, Long, Integer, Integer> value, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                        out.collect(Tuple3.of(value.f1, value.f3, 1));
                    }
                })
                .keyBy(0, 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .timeWindow(Time.seconds(5))
                .sum(2);

        statusStream.print().setParallelism(1);

        DataStream<Tuple5<String, Integer, Long, Integer, Integer>> dataStream = mainDataStream.getSideOutput(outputTag);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerDataStream("log", dataStream, "traceid,userid,timestamp,status,restime,proctime.proctime,rowtime.rowtime");


        String sql = "select pv,avg_res_time,round(CAST(errorcount AS DOUBLE)/pv,2) as errorRate," +
                "(starttime + interval '8' hour ) as stime from (select count(*) as pv,AVG(restime) as avg_res_time  ,"
                + "sum(case when status = 200 then 0 else 1 end) as errorcount, "
                + "TUMBLE_START(rowtime,INTERVAL '1' SECOND)  as starttime "
                + "from log where status <> 404 group by TUMBLE(rowtime,INTERVAL '1' SECOND) )";

        Table result1 = tableEnv.sqlQuery(sql);


        //write to csv sink
        TableSink csvSink = new CsvTableSink("out/log", "|");
        String[] fieldNames = {"a", "b", "c", "d"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.INT, Types.DOUBLE, Types.SQL_TIMESTAMP};
        tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
        result1.insertInto("CsvSinkTable");


        env.execute();
    }
}
