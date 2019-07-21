package relational;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @Author: xu.dm
 * @Date: 2019/7/21 10:03
 * @Description: 类似Hadoop/MapReduce的计数器实现，内置并行计数器
 * This program filters lines from a CSV file with empty fields. In doing so, it counts the number of empty fields per
 * column within a CSV file using a custom accumulator for vectors. In this context, empty fields are those, that at
 * most contain whitespace characters like space and tab.
 *
 * <p>The input file is a plain text CSV file with the semicolon as field separator and double quotes as field delimiters
 * and three columns. See {@link #getDataSet(ExecutionEnvironment, ParameterTool)} for configuration.
 *
 * <p>Usage: <code>EmptyFieldsCountAccumulator --input &lt;path&gt; --output &lt;path&gt;</code> <br>
 *
 * <p>This example shows how to use:
 * <ul>
 * <li>custom accumulators
 * <li>tuple data types
 * <li>inline-defined functions
 * <li>naming large tuple types
 * </ul>
 */
public class EmptyFieldsCountAccumulator {
    private static final String EMPTY_FIELD_ACCUMULATOR= "empty-fields";

    public static void main(String args[]) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get the data set
        final DataSet<StringTriple> file = getDataSet(env, params);

        // filter lines with empty fields
        final DataSet<StringTriple> filteredLines = file.filter(new EmptyFieldFilter());

        // Here, we could do further processing with the filtered lines...
        JobExecutionResult result;
        // output the filtered lines
        if (params.has("output")) {
            filteredLines.writeAsCsv(params.get("output"));
            // execute program
            result = env.execute("Accumulator example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            filteredLines.print();
            result = env.getLastJobExecutionResult();
        }

        // get the accumulator result via its registration key
        final List<Integer> emptyFields = result.getAccumulatorResult(EMPTY_FIELD_ACCUMULATOR);
        System.out.format("Number of detected empty fields per column: %s\n", emptyFields);
    }

    @SuppressWarnings("unchecked")
    private static DataSet<StringTriple> getDataSet(ExecutionEnvironment env, ParameterTool params) {
        if (params.has("input")) {
            return env.readCsvFile(params.get("input"))
                    .fieldDelimiter(";")
                    .pojoType(StringTriple.class);
        } else {
            System.out.println("Executing EmptyFieldsCountAccumulator example with default input data set.");
            System.out.println("Use --input to specify file input.");
            return env.fromCollection(getExampleInputTuples());
        }
    }

    private static Collection<StringTriple> getExampleInputTuples() {
        Collection<StringTriple> inputTuples = new ArrayList<StringTriple>();
        inputTuples.add(new StringTriple("John", "Doe", "Foo Str."));
        inputTuples.add(new StringTriple("Joe", "Johnson", ""));
        inputTuples.add(new StringTriple(null, "Kate Morn", "Bar Blvd."));
        inputTuples.add(new StringTriple("Tim", "Rinny", ""));
        inputTuples.add(new StringTriple("Alicia", "Jackson", "  "));
        inputTuples.add(new StringTriple("Alicia", "Jackson", "  "));
        inputTuples.add(new StringTriple("Alicia", "Jackson", "  "));
        inputTuples.add(new StringTriple("Tom", "Jackson", "A"));
        inputTuples.add(new StringTriple("Amy", "li", "B  "));
        return inputTuples;
    }

    /**
     * This function filters all incoming tuples that have one or more empty fields.
     * In doing so, it also counts the number of empty fields per attribute with an accumulator (registered under
     * {@link EmptyFieldsCountAccumulator#EMPTY_FIELD_ACCUMULATOR}).
     */
    public static final class EmptyFieldFilter extends RichFilterFunction<StringTriple> {

        // create a new accumulator in each filter function instance
        // accumulators can be merged later on
        private final VectorAccumulator emptyFieldCounter = new VectorAccumulator();

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);

            // register the accumulator instance
            getRuntimeContext().addAccumulator(EMPTY_FIELD_ACCUMULATOR,
                    this.emptyFieldCounter);
        }

        @Override
        public boolean filter(final StringTriple t) {
            boolean containsEmptyFields = false;

            // iterate over the tuple fields looking for empty ones
            for (int pos = 0; pos < t.getArity(); pos++) {

                final String field = t.getField(pos);
                if (field == null || field.trim().isEmpty()) {
                    containsEmptyFields = true;

                    // if an empty field is encountered, update the
                    // accumulator
                    this.emptyFieldCounter.add(pos);
                }
            }

            return !containsEmptyFields;
        }
    }

    /**
     * This accumulator maintains a vector of counts. Calling {@link #add(Integer)} increments the
     * <i>n</i>-th vector component. The size of the vector is automatically managed.
     * 这个向量计数器输入是整数，输出是List，并按字段位置计数，List里的索引就是字段计数位置，其值就是计数结果
     */
    public static class VectorAccumulator implements Accumulator<Integer,ArrayList<Integer>>{
        //存储计数器向量
        private final ArrayList<Integer> resultVector;

        public VectorAccumulator() {
            this(new ArrayList<>());
        }

        public VectorAccumulator(ArrayList<Integer> resultVector) {
            this.resultVector = resultVector;
        }

        private void updateResultVector(int position,int delta){
            //如果给出的位置不够就扩充向量容器
            while (this.resultVector.size()<=position){
                this.resultVector.add(0);
            }

            final int component = this.resultVector.get(position);
            this.resultVector.set(position,component+delta);
        }

        //在指定位置加1
        @Override
        public void add(Integer position) {
            updateResultVector(position,1);
        }

        @Override
        public ArrayList<Integer> getLocalValue() {
            return this.resultVector;
        }

        @Override
        public void resetLocal() {
            this.resultVector.clear();
        }

        @Override
        public void merge(Accumulator<Integer, ArrayList<Integer>> other) {
            //合并两个向量计数器容器，按容器的索引合并
            final ArrayList<Integer> otherVector = other.getLocalValue();
            for(int i=0;i<otherVector.size();i++){
                updateResultVector(i,otherVector.get(i));
            }
        }

        @Override
        public Accumulator<Integer, ArrayList<Integer>> clone() {
            return new VectorAccumulator(new ArrayList<>(this.resultVector));
        }

        @Override
        public String toString() {
            return StringUtils.join(this.resultVector,':');
        }
    }

    /**
     * It is recommended to use POJOs (Plain old Java objects) instead of TupleX for
     * data types with many fields. Also, POJOs can be used to give large Tuple-types a name.
     * <a href="https://ci.apache.org/projects/flink/flink-docs-master/apis/best_practices.html#naming-large-tuplex-types">Source (docs)</a>
     */
    public static class StringTriple extends Tuple3<String, String, String> {

        public StringTriple() {}

        public StringTriple(String f0, String f1, String f2) {
            super(f0, f1, f2);
        }

    }

}
