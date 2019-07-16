package ml;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author: xu.dm
 * @Date: 2019/7/16 21:52
 * @Description:
 *  Provides the default data sets used for the Linear Regression example
 *  program. The default data sets are used, if no parameters are given to the program.
 */
public class LinearRegressionData {
    // We have the data as object arrays so that we can also generate Scala Data
    // Sources from it.
    public static final Object[][] PARAMS = new Object[][] { new Object[] {
            0.0, 0.0 } };

    public static final Object[][] DATA = new Object[][] {
            new Object[] { 0.5, 1.0 }, new Object[] { 1.0, 2.0 },
            new Object[] { 2.0, 4.0 }, new Object[] { 3.0, 6.0 },
            new Object[] { 4.0, 8.0 }, new Object[] { 5.0, 10.0 },
            new Object[] { 6.0, 12.0 }, new Object[] { 7.0, 14.0 },
            new Object[] { 8.0, 16.0 }, new Object[] { 9.0, 18.0 },
            new Object[] { 10.0, 20.0 }, new Object[] { -0.08, -0.16 },
            new Object[] { 0.13, 0.26 }, new Object[] { -1.17, -2.35 },
            new Object[] { 1.72, 3.45 }, new Object[] { 1.70, 3.41 },
            new Object[] { 1.20, 2.41 }, new Object[] { -0.59, -1.18 },
            new Object[] { 0.28, 0.57 }, new Object[] { 1.65, 3.30 },
            new Object[] { -0.55, -1.08 } };

    public static DataSet<LinearRegression.Params> getDefaultParamsDataSet(ExecutionEnvironment env) {
        List<LinearRegression.Params> paramsList = new LinkedList<>();
        for (Object[] params : PARAMS) {
            paramsList.add(new LinearRegression.Params((Double) params[0], (Double) params[1]));
        }
        return env.fromCollection(paramsList);
    }

    public static DataSet<LinearRegression.Data> getDefaultDataDataSet(ExecutionEnvironment env) {
        List<LinearRegression.Data> dataList = new LinkedList<>();
        for (Object[] data : DATA) {
            dataList.add(new LinearRegression.Data((Double) data[0], (Double) data[1]));
        }
        return env.fromCollection(dataList);
    }
}
