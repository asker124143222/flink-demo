package clustering;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * @Author: xu.dm
 * @Date: 2019/7/9 16:31
 * @Version: 1.0
 * @Description:
 * K-Means是一种迭代聚类算法，其工作原理如下：
 * K-Means给出了一组要聚类的数据点和一组初始的K聚类中心。
 * 在每次迭代中，算法计算每个数据点到每个聚类中心的距离。每个点都分配给最靠近它的集群中心。
 * 随后，每个聚类中心移动到已分配给它的所有点的中心（平均值）。移动的聚类中心被送入下一次迭代。
 * 该算法在固定次数的迭代之后终止（本例中）或者如果聚类中心在迭代中没有（显着地）移动。
 * 这是K-Means聚类算法的维基百科条目。
 * <a href="http://en.wikipedia.org/wiki/K-means_clustering">
 *
 * 此实现适用于二维数据点。
 * 它计算到集群中心的数据点分配，即每个数据点都使用它所属的最终集群（中心）的id进行注释。
 *
 * 输入文件是纯文本文件，必须格式如下：
 *
 * 数据点表示为由空白字符分隔的两个双精度值。数据点由换行符分隔。
 * 例如，"1.2 2.3\n5.3 7.2\n"给出两个数据点（x = 1.2，y = 2.3）和（x = 5.3，y = 7.2）。
 * 聚类中心由整数id和点值表示。
 * 例如，"1 6.2 3.2\n2 2.9 5.7\n"给出两个中心（id = 1，x = 6.2，y = 3.2）和（id = 2，x = 2.9，y = 5.7）。
 * 用法：KMeans --points <path> --centroids <path> --output <path> --iterations <n>
 * 如果未提供参数，则使用{@link KMeansData}中的默认数据和10次迭代运行程序。
 **/
public class KMeans {
    public static void main(String args[]) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<Point> points =getPointDataSet(params,env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);


    }

    private static DataSet<Point> getPointDataSet(ParameterTool params,ExecutionEnvironment env){
        DataSet<Point> points;
        if(params.has("points")){
            points = env.readCsvFile(params.get("points")).fieldDelimiter(" ")
                    .pojoType(Point.class,"x","y");
        }else{
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = KMeansData.getDefaultPointDataSet(env);
        }
        return points;
    }

    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params,ExecutionEnvironment env){
        DataSet<Centroid> centroids;
        if(params.has("centroids")){
            centroids = env.readCsvFile(params.get("centroids")).fieldDelimiter(" ")
                    .pojoType(Centroid.class,"id","x","y");
        }else{
            System.out.println("Executing K-Means example with default centroid data set.");
            System.out.println("Use --centroids to specify file input.");
            centroids = KMeansData.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }

    /** Determines the closest cluster center for a data point.
     * 找到最近的聚类点
     * */
    public static final class SelectNearestCenter extends RichFlatMapFunction<Point, Tuple2<Integer,Point>>{
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection.
         * 从广播变量里读取聚类中心点数据到集合中
         * */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public void flatMap(Point point, Collector<Tuple2<Integer, Point>> out) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            //检查所有聚类中心
            for(Centroid centroid:centroids){
                //计算点到中心的距离
                double distance = point.euclideanDistance(centroid);

                //更新最小距离
                if(distance<minDistance){
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }
            out.collect(new Tuple2<>(closestCentroidId,point));
        }
    }
}
