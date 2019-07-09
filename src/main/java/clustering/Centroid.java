package clustering;

/**
 * @Author: xu.dm
 * @Date: 2019/7/9 16:28
 * @Version: 1.0
 * @Description: 二维聚类中心点，基于二维坐标类拓展
 **/
public class Centroid extends Point{
    public int id;

    public Centroid() {}

    public Centroid(int id, double x, double y) {
        super(x, y);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.x, p.y);
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString();
    }
}
