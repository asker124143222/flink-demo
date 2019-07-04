import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author: xu.dm
 * @Date: 2019/7/3 11:41
 * @Version: 1.0
 * @Description: 传递闭包算法，本例中就是根据成对路径，查找和生成新的可达路径
 * 例如：1-2，2-4这两对数据，可以得出新的可达路径1-4。
 * <p>
 * 迭代算法步骤：
 * 1、获取成对数据集edges，里面包括路径对，比如 1->2,2->4,2->5等，如果是无向边，还可以反转数据集union之前的数据。本例按有向处理
 * 2、生成迭代头paths
 * 3、用paths和原始数据集edges做join连接，找出头尾相连的数据nextPaths，即类似1->2,2->4这种，然后生成新的路径1->4。
 * 4、新的路径集nextPaths和迭代头数据集paths进行并集操作，即union操作，生成新的nextPaths,这个时候它包含了新旧两种数据
 * 在这里总是nextPaths>=paths
 * 5、去重操作，第一次迭代不会重复，但是第二次迭代开始就会有重复数据，通过groupBy全字段，去分组第一条即可达到去重效果
 * 6、以上核心迭代体完成，后面需要形成迭代闭环，确定迭代退出条件
 * 7、退出原理：每次迭代完成后，需要检查是否新的路径产生，如果没有则表示迭代可以结束
 * 8、可达寻路步骤完成后，通过对比nextPaths和paths，如果nextPaths>paths，表示有新路径生成，需要继续迭代，直到nextPaths=paths
 * 9、这里有一个迭代重要的概念，paths和nextPaths是通过迭代闭环不断更新的
 * 10、本例中迭代头和迭代尾的数据流向：paths->nextPaths->paths.
 * 11、本例通过bulk迭代方式实现了delta迭代的效果
 **/
public class TransitiveClosureNaive {
    public static void main(String args[]) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final int maxIterations = params.getInt("iterations", 10);

        DataSet<Tuple2<Long, Long>> edges;
        if (params.has("edges")) {
            edges = env.readCsvFile(params.get("edges")).fieldDelimiter(" ").types(Long.class, Long.class);
        } else {
            System.out.println("Executing TransitiveClosureNaive example with default edges data set.");
            System.out.println("Use --edges to specify file input.");
            edges = ConnectedComponentsData.getDefaultEdgeDataSet(env);
        }

        IterativeDataSet<Tuple2<Long, Long>> paths = edges.iterate(maxIterations);

        DataSet<Tuple2<Long, Long>> nextPaths = paths
                .join(edges)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    /**
                     left: Path (z,x) - x is reachable by z
                     right: Edge (x,y) - edge x-->y exists
                     out: Path (z,y) - y is reachable by z
                     */
                    @Override
                    public Tuple2<Long, Long> join(Tuple2<Long, Long> left, Tuple2<Long, Long> right) throws Exception {
                        return new Tuple2<>(left.f0, right.f1);
                    }
                })
                //类似withForwardedFieldsFirst这种无损转发语义声明，可选，有助于提高flink优化器生成更高效的执行计划
                //转发第一个输入Tuple2<Long, Long>中的第一个字段，转发第二个输入Tuple2<Long, Long>中的第二个字段
                .withForwardedFieldsFirst("0").withForwardedFieldsSecond("1")
                //合并原有的路径
                .union(paths)
                //这里的groupBy两个fields是打算给reduceGroup去重使用
                .groupBy(0, 1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
                        out.collect(values.iterator().next());
                    }
                })
                .withForwardedFields("0;1");

        //对比paths以及新生成的nextPaths，获取nextPaths中比paths多的路径
        //从上面的算子可以得知,nextPaths总是大于或等于paths
        DataSet<Tuple2<Long, Long>> newPaths = paths
                .coGroup(nextPaths)
                .where(0).equalTo(0)
                .with(new CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    Set<Tuple2<Long, Long>> prevSet = new HashSet<>();

                    @Override
                    public void coGroup(Iterable<Tuple2<Long, Long>> prevPaths, Iterable<Tuple2<Long, Long>> nextPaths, Collector<Tuple2<Long, Long>> out) throws Exception {
                        for (Tuple2<Long, Long> prev : prevPaths) {
                            prevSet.add(prev);
                        }
                        //检查有没有新的数据产生，如果有就继续迭代，否则迭代终止
                        for (Tuple2<Long, Long> next : nextPaths) {
                            if (!prevSet.contains(next)) {
                                out.collect(next);
                            }
                        }
                    }
                }).withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");

        //迭代尾，在这里形成闭环，nextPaths是反馈通道，nextPaths数据集被重新传递到迭代头paths里，然后通过迭代算子不断执行。
        //newPaths为空或者迭代达到最大次数，迭代结束。newPaths这里表示是否有新的路径。
        //数据集迭代环：paths->nextPaths->paths
        DataSet<Tuple2<Long, Long>> transitiveClosure = paths.closeWith(nextPaths, newPaths);

        // emit result
        if (params.has("output")) {
            transitiveClosure.writeAsCsv(params.get("output"), "\n", " ");

            // execute program explicitly, because file sinks are lazy
            env.execute("Transitive Closure Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            transitiveClosure.print();
        }
    }
}
