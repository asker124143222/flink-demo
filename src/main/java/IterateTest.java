import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author: xu.dm
 * @Date: 2019/7/4 0:18
 * @Description:
 */
public class IterateTest {
    public static void main(String args[]) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        final int maxIterations = params.getInt("iterations", 2);

        DataSet<Integer> edges = env.fromElements(1,2,3,4,5);


        IterativeDataSet<Integer> paths = edges.iterate(maxIterations);

        DataSet<Integer> nextPaths = paths
               .map(new MapFunction<Integer, Integer>() {
                   @Override
                   public Integer map(Integer value) throws Exception {
                       return value + 1;
                   }
               });


        DataSet<Integer> transitiveClosure = paths.closeWith(nextPaths);

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
