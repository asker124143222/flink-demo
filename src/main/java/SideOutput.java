import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: xu.dm
 * @Date: 2019/6/17 22:06
 * @Description: 侧输出(sideoutput)
 */
public class SideOutput {
    private static final OutputTag<String> rejectWordsTag = new OutputTag<String>("rejected"){};

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // 获取输入数据
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // normalize and split the line
                        String[] tokens = value.toLowerCase().split("\\W+");

                        // emit the pairs
                        for(String token : tokens){
                            if(token.length()>5){
                                ctx.output(rejectWordsTag,token);
                                out.collect(new Tuple2<>("rejectWordCount",1));
                            }else if(token.length()>0){
                                out.collect(new Tuple2<>(token,1));

                            }
                            else {
                                out.collect(new Tuple2<>("error",1));
                            }
                        }
                    }
                });


        // 获取侧输出
        DataStream<String> rejectedWords = tokenized.getSideOutput(rejectWordsTag)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "rejected:" + value;
                    }
                });

        DataStream<Tuple2<String, Integer>> counts = tokenized
                .keyBy(0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        // wordcount结果输出
        counts.print();
        // 侧输出结果输出
        rejectedWords.print();

        // execute program
        env.execute("Streaming WordCount SideOutput");

    }

    /**
     * 以用户自定义FlatMapFunction函数的形式来实现分词器功能，该分词器会将分词封装为(word,1)，
     * 同时不接受单词长度大于5的，也即是侧输出都是单词长度大于5的单词。
     */
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String,Integer>>{
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for(String token : tokens){
                if(token.length()>5){
                    ctx.output(rejectWordsTag,token);
                }else if(token.length()>0){
                    out.collect(new Tuple2<>(token,1));
                }
                else {
                    out.collect(new Tuple2<>("error",1));
                }
            }
        }
    }


}
