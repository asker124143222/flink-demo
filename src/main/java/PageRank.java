import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @Author: xu.dm
 * @Date: 2019/6/3 20:10
 * @Description:
 */
public class PageRank {
    //阻尼系数
    private static final double DAMPENING_FACTOR = 0.85;
    private static final double EPSILON = 0.0001;

    private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params){
        if(params.has("pages")){
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
        }
        else{
            System.out.println("Executing PageRank example with default pages data set.");
            System.out.println("Use --pages to specify file input.");
            return PageRankData.getDefaultPagesDataSet(env);
        }
    }

    private static DataSet<Tuple2<Long,Long>> getLinksDataSet(ExecutionEnvironment env,ParameterTool params){
        if(params.has("links")){
            return env.readCsvFile(params.get("links"))
                   .fieldDelimiter(" ")
                   .lineDelimiter("\n")
                   .types(Long.class,Long.class);
        }
        else{
            System.out.println("Executing PageRank example with default links data set.");
            System.out.println("Use --links to specify file input.");
            return PageRankData.getDefaultEdgeDataSet(env);
        }
    }

    public static void main(String[] args) throws Exception{
        ParameterTool params = ParameterTool.fromArgs(args);
        final int numPages = params.getInt("numPages", PageRankData.getNumberOfPages());
        final int maxIterations = params.getInt("iterations", 10);

        // set up execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make the parameters available to the web ui
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<Long> pagesInput = getPagesDataSet(env,params);
        DataSet<Tuple2<Long,Long>> linksInput = getLinksDataSet(env,params);
    }
}
