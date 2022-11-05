package cn.edu.tsinghua.iginx.integration.expansion.InfluxDB;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBHistoryDataGeneratorTest extends BaseHistoryDataGenerator {

    public static final String TOKEN = "testToken";

    public static final String URL = "http://localhost:8086/";

    public static final String ORGANIZATION = "testOrg";

    private static final WritePrecision WRITE_PRECISION = WritePrecision.MS;

    @Test
    public void writeHistoryData() {
        InfluxDBClient client = InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORGANIZATION);

        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> ORGANIZATION.equals(o.getName()))
                .findFirst()
                .orElseThrow(IllegalAccessError::new);

        client.getBucketsApi().createBucket("data_center", organization);

        List<Point> points = new ArrayList<>();

        long timestamp = 1000 * 1000;

        Map<String, String> tags = new HashMap<>();
        tags.put("host", "1");
        tags.put("rack", "A");
        tags.put("room", "ROOMA");
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 66.3).addField("temperature", 56.4)
                .time(timestamp, WRITE_PRECISION));
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 67.1).addField("temperature", 56.2)
                .time(timestamp + 1000 * 300, WRITE_PRECISION));

        tags = new HashMap<>();
        tags.put("host", "2");
        tags.put("rack", "B");
        tags.put("room", "ROOMA");
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 72.1).addField("temperature", 55.1)
                .time(timestamp, WRITE_PRECISION));

        tags = new HashMap<>();
        tags.put("host", "4");
        tags.put("rack", "B");
        tags.put("room", "ROOMB");
        points.add(Point.measurement("cpu").addTags(tags)
                .addField("usage", 22.1).addField("temperature", 99.8)
                .time(timestamp + 1000 * 300, WRITE_PRECISION));


        client.getWriteApiBlocking().writePoints("data_center", organization.getId(), points);
        client.close();
    }

}
