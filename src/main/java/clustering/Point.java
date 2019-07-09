package clustering;

/**
 * @Author: xu.dm
 * @Date: 2019/7/9 16:25
 * @Version: 1.0
 * @Description: 二维坐标点
 **/
public class Point {
    public double x, y;

    public Point() {}

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point add(Point other) {
        x += other.x;
        y += other.y;
        return this;
    }

    //取均值使用
    public Point div(long val) {
        x /= val;
        y /= val;
        return this;
    }

    //欧几里得距离
    public double euclideanDistance(Point other) {
        return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
    }

    public void clear() {
        x = y = 0.0;
    }

    @Override
    public String toString() {
        return x + " " + y;
    }
}
