import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;


import org.apache.flink.util.Collector;

/**
 * @Author: xu.dm
 * @Date: 2019/7/4 21:31
 * @Description: 三角枚举算法
 * 三角枚举是在图（数据结构）中找到紧密连接的部分的预处理步骤。三角形由三条边连接，三条边相互连接。
 *
 * 该算法的工作原理如下：它将所有共享一个共同顶点的边(edge)分组，并构建三元组，即由两条边连接的顶点三元组。
 * 最后，通过join操作连接原数据和三元组，过滤所有三元组，去除不存在第三条边(闭合三角形)的三元组。
 *
 * 输入文件是纯文本文件，必须格式如下：
 *
 * 边缘表示为顶点ID的对，由空格字符分隔。边线由换行符分隔。
 * 例如，"1 2\n2 12\n1 12\n42 63"给出包括三角形的四个（无向）边（1） - （2），（2） - （12），（1） - （12）和（42） - （63）
 *     （1）
 *     /   \
 *  （2）-（12）
 * 用法：EnumTriangleBasic --edges <path> --output <path>
 * 如果未提供参数，则使用{@link EnumTrianglesData}中的默认数据运行程序。
 *
 */
public class EnumTriangles {
    public static void main(String args[]) throws Exception{
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // read input data
        DataSet<EnumTrianglesDataTypes.Edge> edges;
        if (params.has("edges")) {
            edges = env.readCsvFile(params.get("edges"))
                    .fieldDelimiter(" ")
                    .includeFields(true, true)
                    .types(Integer.class, Integer.class)
                    .map(new TupleEdgeConverter());
        } else {
            System.out.println("Executing EnumTriangles example with default edges data set.");
            System.out.println("Use --edges to specify file input.");
            edges = EnumTrianglesData.getDefaultEdgeDataSet(env);
        }
    }

    //转换Tuple2类型到Edge类型
    @FunctionAnnotation.ForwardedFields("0;1")
    public static class TupleEdgeConverter implements MapFunction<Tuple2<Integer,Integer>, EnumTrianglesDataTypes.Edge>{
        private final EnumTrianglesDataTypes.Edge outEdge = new EnumTrianglesDataTypes.Edge();
        @Override
        public EnumTrianglesDataTypes.Edge map(Tuple2<Integer, Integer> value) throws Exception {
            outEdge.copyVerticesFromTuple2(value);
            return outEdge;
        }
    }

    /**
     * Projects an edge (pair of vertices) such that the id of the first is smaller than the id of the second.
     * 转换一对边，如果first大于second，则互相交换
     * */
    private static class EdgeByIdProjector implements MapFunction<EnumTrianglesDataTypes.Edge, EnumTrianglesDataTypes.Edge>{
        @Override
        public EnumTrianglesDataTypes.Edge map(EnumTrianglesDataTypes.Edge value) throws Exception {

            //flip vertices 反转顶点
            if(value.getFirstVertex()>value.getSecondVertex())
            {
                value.flipVertices();
            }
            return value;
        }
    }

    private static class TriadBuilder implements GroupReduceFunction<EnumTrianglesDataTypes.Edge, EnumTrianglesDataTypes.Triad>{
        @Override
        public void reduce(Iterable<EnumTrianglesDataTypes.Edge> values, Collector<EnumTrianglesDataTypes.Triad> out) throws Exception {

        }
    }

}
