import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * @Author: xu.dm
 * @Date: 2019/6/12 20:22
 * @Description:
 */
public class IteratePi {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create initialIterativeDataSet
        IterativeDataSet<Integer> inital = env.fromElements(0).iterate(100);

        DataSet<Integer> iteration = inital.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                double x = Math.random();
                double y = Math.random();

                Integer result = value + ((x * x + y * y < 1) ? 1 : 0);

                System.out.println("iteration:"+result);
                return result;
            }
        });


        // Iteratively transform the IterativeDataSet
        //迭代100次或者达到收敛条件（value>=50）
        DataSet<Integer> count = inital.closeWith(iteration,iteration.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value<50;
            }
        }));


        System.out.println("pi:");
        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer value) throws Exception {
                Double result = value /(double) 10000 * 4;
                return result;
            }
        }).print();

    }

}
