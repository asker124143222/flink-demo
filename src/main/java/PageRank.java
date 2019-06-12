import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * A basic implementation of the Page Rank algorithm using a bulk iteration.
 *
 * <p>This implementation requires a set of pages and a set of directed links as input and works as follows. <br>
 * In each iteration, the rank of every page is evenly distributed to all pages it points to.
 * Each page collects the partial ranks of all pages that point to it, sums them up, and applies a dampening factor to the sum.
 * The result is the new rank of the page. A new iteration is started with the new ranks of all pages.
 * This implementation terminates after a fixed number of iterations.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/Page_rank">Page Rank algorithm</a>.
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Pages represented as an (long) ID separated by new-line characters.<br>
 * For example <code>"1\n2\n12\n42\n63"</code> gives five pages with IDs 1, 2, 12, 42, and 63.
 * <li>Links are represented as pairs of page IDs which are separated by space
 * characters. Links are separated by new-line characters.<br>
 * For example <code>"1 2\n2 12\n1 12\n42 63"</code> gives four (directed) links (1)-&gt;(2), (2)-&gt;(12), (1)-&gt;(12), and (42)-&gt;(63).<br>
 * For this simple implementation it is required that each page has at least one incoming and one outgoing link (a page can point to itself).
 * </ul>
 *
 * <p>Usage: <code>PageRankBasic --pages &lt;path&gt; --links &lt;path&gt; --output &lt;path&gt; --numPages &lt;n&gt; --iterations &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {PageRankData} and 10 iterations.
 *
 * <p>This example shows how to use:
 * <ul>
 * <li>Bulk Iterations
 * <li>Default Join
 * <li>Configure user-defined functions using constructor parameters.
 * </ul>
 */
public class PageRank {
    //阻尼系数
    private static final double DAMPENING_FACTOR = 0.85;
    //收敛阈值.
    private static final double EPSILON = 0.0001;

    private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params) {
        if (params.has("pages")) {
            return env.readCsvFile(params.get("params"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class)
                    .map(new MapFunction<Tuple1<Long>, Long>() {
                        @Override
                        public Long map(Tuple1<Long> value) throws Exception {
                            return value.f0;
                        }
                    });
        } else {
            System.out.println("Executing PageRank example with default pages data set.");
            System.out.println("Use --pages to specify file input.");
            return PageRankData.getDefaultPagesDataSet(env);
        }
    }

    private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool params) {
        if (params.has("links")) {
            return env.readCsvFile(params.get("links"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class);
        } else {
            System.out.println("Executing PageRank example with default links data set.");
            System.out.println("Use --links to specify file input.");
            return PageRankData.getDefaultEdgeDataSet(env);
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final int numPages = params.getInt("numPages", PageRankData.getNumberOfPages());
        final int maxIterations = params.getInt("iterations", 10);

        // set up execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make the parameters available to the web ui
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<Long> pagesInput = getPagesDataSet(env, params);
        DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env, params);

        // assign initial rank to pages
        DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput
                .map(new RankAssigner(1.0d / numPages));

        // build adjacency list from link input
        DataSet<Tuple2<Long, Long[]>> adjacencyListInput = linksInput
                .groupBy(0)
                .reduceGroup(new BuildOutgoingEdgeList());//.setParallelism(2);

        // set iterative data set
        IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

        DataSet<Tuple2<Long, Double>> newRanks = iteration.join(adjacencyListInput)
                .where(0).equalTo(0)
                //迭代算子
                .flatMap(new JoinVertexWithEdgesMatch())
                // collect and sum ranks
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1)
                // apply dampening factor
                .map(new Dampener(PageRank.DAMPENING_FACTOR, numPages));


        DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
                newRanks,
                newRanks.join(iteration).where(0).equalTo(0)
                        // termination condition
                        .filter(new EpsilonFilter()));

        // emit result
        if (params.has("output")) {
            finalPageRanks.writeAsCsv(params.get("output"), "\n", " ");
            // execute program
            env.execute("Basic Page Rank Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            finalPageRanks.print();
        }
    }

    /**
     * A map function that assigns an initial rank to all pages.
     */
    public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
        Tuple2<Long, Double> outPageWithRank;

        public RankAssigner(double rank) {
            this.outPageWithRank = new Tuple2<>(-1L, rank);
        }

        @Override
        public Tuple2<Long, Double> map(Long value) throws Exception {
            outPageWithRank.f0 = value;
            return outPageWithRank;
        }
    }

    /**
     * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
     * originate. Run as a pre-processing step.
     * 将分组后的links数据按Id放入Tuple2<Long, Long[]>
     * 与hadoop的mapreduce的reduce部分类似，groupby就是shuffle
     * reduce过程可以是并行的
     */
    @FunctionAnnotation.ForwardedFields("0")
    public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

        private final ArrayList<Long> neighbors = new ArrayList<Long>();

        @Override
        public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
            neighbors.clear();
            Long id = 0L;

            for (Tuple2<Long, Long> n : values) {
                id = n.f0;
                neighbors.add(n.f1);
                System.out.println("id: " + id + " ,neighbors: " + n.f1);
            }
            out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
//            System.out.println("reduce end -->id:"+id);
        }
    }

    /**
     * Join function that distributes a fraction of a vertex's rank to all neighbors.
     * 按照页面id以及其连接页面的数量，重新计算相邻点的rank，迭代10次
     * rankToDistribute就是计算了顶点（id）指向边（neighbors）的贡献值，neighbors最终所获得的rank值需要合计后才能算出
     */
    public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

        @Override
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out) {
            System.out.println("before:" + value);
            Long[] neighbors = value.f1.f1;
            double rank = value.f0.f1;
            double rankToDistribute = rank / ((double) neighbors.length);

            for (Long neighbor : neighbors) {
//                System.out.println("neighbor:" + neighbor + ",rankToDistribute:" + rankToDistribute);
                out.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
            }
        }
    }

    /**
     * The function that applies the page rank dampening formula.
     * 阻尼系数公式:PR(A)=(1-d)/N + d(PR(T1)/C(T1)+ ... +PR(Tn)/C(Tn))
     * PR(A) 是页面A的PR值
     * PR(Ti)是页面Ti的PR值，在这里，页面Ti是指向A的所有页面中的某个页面
     * C(Ti)是页面Ti的出度，也就是Ti指向其他页面的边的个数
     * d 为阻尼系数，其意义是，在任意时刻，用户到达某页面后并继续向后浏览的概率，
     * 该数值是根据上网者使用浏览器书签的平均频率估算而得，通常d=0.85
     */
    @FunctionAnnotation.ForwardedFields("0")
    public static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private final double dampening;
        private final double randomJump;

        public Dampener(double dampening, double numVertices) {
            this.dampening = dampening;
            this.randomJump = (1 - dampening) / numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
            value.f1 = (value.f1 * dampening) + randomJump;
            return value;
        }
    }

    /**
     * Filter that filters vertices where the rank difference is below a threshold.
     * 如果迭代之间的参数之差低于此EPSILON，我们将会收敛
     * 每次迭代后都会调用Filter判断是否要退出迭代
     * FilterFunction<T>里的T是由newRanks.join(iteration).where(0).equalTo(0)后的数据，
     * 每次迭代newRanks和iteration这两个数据集都更新
     */
    public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

        @Override
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
            System.out.println("value:"+value+",math:"+Math.abs(value.f0.f1 - value.f1.f1));
            return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
        }
    }

}
