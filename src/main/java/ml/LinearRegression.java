package ml;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Collection;

/**
 * @Author: xu.dm
 * @Date: 2019/7/16 21:52
 * @Description: 批量梯度下降算法解决线性回归 y = theta0 + theta1*x 的参数求解。
 * 本例实现一元数据求解二元参数。
 * BGD（批量梯度下降）算法的线性回归是一种迭代聚类算法，其工作原理如下：
 * BGD给出了数据集和目标集，试图找出适合目标集的数据集的最佳参数。
 * 在每次迭代中，算法计算代价函数（cost function）的梯度并使用它来更新所有参数。
 * 算法在固定次数的迭代后终止（如本实现中所示）通过足够的迭代，算法可以最小化成本函数并找到最佳参数。
 * Linear Regression with BGD(batch gradient descent) algorithm is an iterative clustering algorithm and works as follows:
 * Giving a data set and target set, the BGD try to find out the best parameters for the data set to fit the target set.
 * In each iteration, the algorithm computes the gradient of the cost function and use it to update all the parameters.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * With enough iteration, the algorithm can minimize the cost function and find the best parameters
 *
 * This implementation works on one-dimensional data. And find the two-dimensional theta.
 * It find the best Theta parameter to fit the target.
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character. The first one represent the X(the training data) and the second represent the Y(target).
 * Data points are separated by newline characters.<br>
 * For example <code>"-0.02 -0.04\n5.3 10.6\n"</code> gives two data points (x=-0.02, y=-0.04) and (x=5.3, y=10.6).
 * </ul>
 */
public class LinearRegression {
    public static void main(String args[]) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        final int iterations = params.getInt("iterations", 10);

        // get input x data from elements
        DataSet<Data> data;
        if (params.has("input")) {
            // read data from CSV file
            data = env.readCsvFile(params.get("input"))
                    .fieldDelimiter(" ")
                    .includeFields(true, true)
                    .pojoType(Data.class);
        } else {
            System.out.println("Executing LinearRegression example with default input data set.");
            System.out.println("Use --input to specify file input.");
            data = LinearRegressionData.getDefaultDataDataSet(env);
        }

        // get the parameters from elements
        DataSet<Params> parameters = LinearRegressionData.getDefaultParamsDataSet(env);

        // set number of bulk iterations for SGD linear Regression
        IterativeDataSet<Params> loop = parameters.iterate(iterations);

        DataSet<Params> newParameters = data
                // compute a single step using every sample
                .map(new SubUpdate()).withBroadcastSet(loop,"parameters").setParallelism(1)
                // sum up all the steps
                .reduce(new UpdateAccumulator())
                // average the steps and update all parameters
                .map(new Update());

        // feed new parameters back into next iteration
        DataSet<Params> result = loop.closeWith(newParameters);

        // emit result
        if (params.has("output")) {
            result.writeAsText(params.get("output"));
            // execute program
            env.execute("Linear Regression example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }

    }


    /**
     * A simple data sample, x means the input, and y means the target.
     */
    public static class Data implements Serializable{
        public double x, y;

        public Data() {}

        public Data(double x, double y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return "(" + x + "|" + y + ")";
        }

    }

    /**
     * A set of parameters -- theta0, theta1.
     */
    public static class Params implements Serializable {

        private double theta0, theta1;

        public Params() {}

        public Params(double x0, double x1) {
            this.theta0 = x0;
            this.theta1 = x1;
        }

        @Override
        public String toString() {
            return theta0 + " " + theta1;
        }

        public double getTheta0() {
            return theta0;
        }

        public double getTheta1() {
            return theta1;
        }

        public void setTheta0(double theta0) {
            this.theta0 = theta0;
        }

        public void setTheta1(double theta1) {
            this.theta1 = theta1;
        }

        public Params div(Integer a) {
            this.theta0 = theta0 / a;
            this.theta1 = theta1 / a;
            return this;
        }

    }

    /**
     * Compute a single BGD type update for every parameters.
     * h(x) = theta0*X0 + theta1*X1，假设X0=1,则h(x) = theta0 + theta1*X1,即y = theta0 + theta1*x
     * 代价函数：j=h(x)-y，这里用的是比较简单的cost function
     * theta0 = theta0 - α∑(h(x)-y)
     * theta1 = theta1 - α∑((h(x)-y)*x)
     *
     */
    public static class SubUpdate extends RichMapFunction<Data, Tuple2<Params, Integer>> {

        private Collection<Params> parameters;

        private Params parameter;

        private int count = 1;

        /** Reads the parameters from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.parameters = getRuntimeContext().getBroadcastVariable("parameters");
        }

        @Override
        public Tuple2<Params, Integer> map(Data in) throws Exception {

            for (Params p : parameters){
                this.parameter = p;
            }
            //核心计算，对于y = theta0 + theta1*x 假定theta0乘以X0=1，所以theta0计算不用乘以in.x
            System.out.println("parameter: "+parameter+" , data:"+in);
            double theta0 = parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in.x)) - in.y);
            double theta1 = parameter.theta1 - 0.01 * (((parameter.theta0 + (parameter.theta1 * in.x)) - in.y) * in.x);
            System.out.println("theta0: "+theta0+" , theta1: "+theta1);

            return new Tuple2<>(new Params(theta0, theta1), count);
        }
    }

    /**
     * Accumulator all the update.
     * */
    public static class UpdateAccumulator implements ReduceFunction<Tuple2<Params, Integer>> {

        @Override
        public Tuple2<Params, Integer> reduce(Tuple2<Params, Integer> val1, Tuple2<Params, Integer> val2) {

            double newTheta0 = val1.f0.theta0 + val2.f0.theta0;
            double newTheta1 = val1.f0.theta1 + val2.f0.theta1;
            Params result = new Params(newTheta0, newTheta1);
            return new Tuple2<>(result, val1.f1 + val2.f1);

        }
    }

    /**
     * Compute the final update by average them.
     */
    public static class Update implements MapFunction<Tuple2<Params, Integer>, Params> {

        @Override
        public Params map(Tuple2<Params, Integer> arg0) throws Exception {

            return arg0.f0.div(arg0.f1);

        }

    }
}
