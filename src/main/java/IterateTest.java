import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author: xu.dm
 * @Date: 2019/7/4 0:18
 * @Description: 目的是为了了解迭代尾是如何传递给迭代头的。
 */
public class IterateTest {
    public static void main(String args[]) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        final int maxIterations = params.getInt("iterations", 2);

        DataSet<Integer> edges = env.fromElements(1, 2, 3, 4, 5);


        //迭代头
        IterativeDataSet<Integer> paths = edges.iterate(maxIterations);

        DataSet<Integer> nextPaths = paths
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value + 1;
                    }
                });

        //本例中newPaths总是能输出一个数字，即nextPaths中的最大值
        DataSet<Integer> newPaths = paths
                .coGroup(nextPaths)
                .where(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .equalTo(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .with(new CoGroupFunction<Integer, Integer, Integer>() {
                    Set<Integer> prevSet = new HashSet<>();
                    @Override
                    public void coGroup(Iterable<Integer> first, Iterable<Integer> second, Collector<Integer> out) throws Exception {
                        System.out.println("----------------");
                        for(Integer f:first){
                            System.out.println("first: "+f);
                            prevSet.add(f);
                        }
                        for(Integer f2:second){
                            if(!prevSet.contains(f2))
                                System.out.println("second: "+f2);
                                out.collect(f2);
                        }
                    }
                }).setParallelism(1);

        //迭代尾，在这里形成闭环，nextPaths是反馈通道，nextPaths数据集被重新传递到迭代头paths里，然后通过迭代算子，不断执行。
        //数据集迭代环：paths->nextPaths->paths
        DataSet<Integer> transitiveClosure = paths.closeWith(nextPaths,newPaths);

        // emit result
        if (params.has("output")) {
            transitiveClosure.writeAsCsv(params.get("output"), "\n", " ");

            // execute program explicitly, because file sinks are lazy
            env.execute("IterateTest Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            transitiveClosure.print();
        }
    }
}
