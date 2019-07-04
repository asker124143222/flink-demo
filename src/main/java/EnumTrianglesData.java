import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xu.dm
 * @Date: 2019/7/4 21:28
 * @Description:
 */
public class EnumTrianglesData {
    public static final Object[][] EDGES = {
            {1, 2},
            {1, 3},
            {1, 4},
            {1, 5},
            {2, 3},
            {2, 5},
            {3, 4},
            {3, 7},
            {3, 8},
            {5, 6},
            {7, 8}
    };

    public static DataSet<EnumTrianglesDataTypes.Edge> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<EnumTrianglesDataTypes.Edge> edges = new ArrayList<EnumTrianglesDataTypes.Edge>();
        for (Object[] e : EDGES) {
            edges.add(new EnumTrianglesDataTypes.Edge((Integer) e[0], (Integer) e[1]));
        }

        return env.fromCollection(edges);
    }
}
