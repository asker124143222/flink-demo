import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

/**
 * @Author: xu.dm
 * @Date: 2019/6/18 21:12
 * @Description: 产生数据 traceid,userid,timestamp,status,response time
 */
public class SourceData implements SourceFunction<String> {
    private volatile boolean Running = true;
    static int status[] = {200, 404, 500, 501, 301};

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (Running) {
            Thread.sleep((int) Math.random() * 100);

            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(UUID.randomUUID().toString());
            stringBuffer.append(",");
            stringBuffer.append((int) Math.random() * 1000);
            stringBuffer.append(",");
            stringBuffer.append(System.currentTimeMillis());
            stringBuffer.append(",");
            stringBuffer.append(status[(int) Math.random() * 4]);
            stringBuffer.append(",");
            stringBuffer.append((int)Math.random()*200);

            ctx.collect(stringBuffer.toString());
        }
    }

    @Override
    public void cancel() {

    }
}
