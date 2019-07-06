import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;


import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

        // project edges by vertex id
        // 对Edge里V1,V2排序，让小的在前
        DataSet<EnumTrianglesDataTypes.Edge> edgesById = edges
                .map(new EdgeByIdProjector());

        DataSet<EnumTrianglesDataTypes.Triad> triangles = edgesById
                // build triads
                .groupBy(EnumTrianglesDataTypes.Edge.V1)
                .sortGroup(EnumTrianglesDataTypes.Edge.V2, Order.ASCENDING)
                .reduceGroup(new TriadBuilder())
                //通过Triad的第二和第三字段与Edge的第一和第二字段join，找出有第二和第三有路径的对子。
                .join(edgesById)
                .where(EnumTrianglesDataTypes.Triad.V2, EnumTrianglesDataTypes.Triad.V3)
                .equalTo(EnumTrianglesDataTypes.Edge.V1, EnumTrianglesDataTypes.Edge.V2)
                // filter triads
                .with(new TriadFilter());

        // emit result
        if (params.has("output")) {
            triangles.writeAsCsv(params.get("output"), "\n", ",");
            // execute program
            env.execute("Basic Triangle Enumeration Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            triangles.print();
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

    /**
     *  Builds triads (triples of vertices) from pairs of edges that share a vertex.
     *  The first vertex of a triad is the shared vertex, the second and third vertex are ordered by vertexId.
     *  Assumes that input edges share the first vertex and are in ascending order of the second vertex.
     *  构造triads（三位一体结构），即经过分组排序后，拥有共同顶点的edge，按顺序（分组后顶点需要升序）组合成三体
     *  构成Triad的第一个顶点是分组id，也是分组里所有对子（edge）共享的id，第二和第三个顶点是分组的对子（edge）里的第二个顶点，edge他们是按升序排列。
     *  分组结构类似如下，s1就是共享的顶点，f1到f3是按升序排列的对子(s1-f1),(s1-f2),(f1-f3)。
     *       f1
     *     /
     *  s1 一 f2
     *     \
     *     f3
     */
    @FunctionAnnotation.ForwardedFields("0")
    private static class TriadBuilder
            implements GroupReduceFunction<EnumTrianglesDataTypes.Edge, EnumTrianglesDataTypes.Triad>{
        private final List<Integer> vertices = new ArrayList<>();
        private final EnumTrianglesDataTypes.Triad outTriad = new EnumTrianglesDataTypes.Triad();

        @Override
        public void reduce(Iterable<EnumTrianglesDataTypes.Edge> values, Collector<EnumTrianglesDataTypes.Triad> out) throws Exception {
            final Iterator<EnumTrianglesDataTypes.Edge> edges = values.iterator();

            // clear vertex list
            vertices.clear();

            // read first edge
            EnumTrianglesDataTypes.Edge firstEdge = edges.next();
            outTriad.setFirstVertex(firstEdge.getFirstVertex());

            vertices.add(firstEdge.getSecondVertex());

            // build and emit triads
            if(edges.hasNext()){
                Integer higherVertexId = edges.next().getSecondVertex();

                //combine vertex with all previously read vertices
                for(Integer lowVertexId:vertices){
                    outTriad.setSecondVertex(lowVertexId);
                    outTriad.setThirdVertex(higherVertexId);
                    out.collect(outTriad);
                }
                vertices.add(higherVertexId);
            }
        }
    }

    /**
     * Filters triads (three vertices connected by two edges) without a closing third edge.
     * 过滤没有闭合边的三体
     * */
    private static class TriadFilter
            implements JoinFunction<EnumTrianglesDataTypes.Triad, EnumTrianglesDataTypes.Edge, EnumTrianglesDataTypes.Triad>{
        @Override
        public EnumTrianglesDataTypes.Triad join(EnumTrianglesDataTypes.Triad first, EnumTrianglesDataTypes.Edge second) throws Exception {
            return first;
        }
    }

}
