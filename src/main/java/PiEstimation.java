import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @Author: xu.dm
 * @Date: 2019/7/7 17:28
 * @Description: π值估算
 *  Estimates the value of Pi using the Monte Carlo method.
 *  The area of a circle is Pi * R^2, R being the radius of the circle
 *  The area of a square is 4 * R^2, where the length of the square's edge is 2*R.
 *
 *  <p>Thus Pi = 4 * (area of circle / area of square).
 *
 *  <p>The idea is to find a way to estimate the circle to square area ratio.
 *  The Monte Carlo method suggests collecting random points (within the square)
 *  and then counting the number of points that fall within the circle
 *
 *  <pre>
 *  {@code
 *  x = Math.random()
 *  y = Math.random()
 *
 *  x * x + y * y < 1
 *  }
 *  </pre>
 */
public class PiEstimation {
    public static void main(String args[]) throws Exception {
        final long numSamples = args.length > 0 ? Long.parseLong(args[0]) : 1000000;

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // count how many of the samples would randomly fall into
        // the unit circle
        DataSet<Long> count =
                env.generateSequence(1, numSamples)
                        .map(new Sampler())
                        .reduce(new SumReducer());

        long theCount = count.collect().get(0);

        System.out.println("We estimate Pi to be: " + (theCount * 4.0 / numSamples));
    }


    /**
     * Sampler randomly emits points that fall within a square of edge x * y.
     * It calculates the distance to the center of a virtually centered circle of radius x = y = 1
     * If the distance is less than 1, then and only then does it returns a 1.
     */
    public static class Sampler implements MapFunction<Long, Long> {

        @Override
        public Long map(Long value) {
            double x = Math.random();
            double y = Math.random();
            return (x * x + y * y) < 1 ? 1L : 0L;
        }
    }


    /**
     * Simply sums up all long values.
     */
    public static final class SumReducer implements ReduceFunction<Long>{

        @Override
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }
}
