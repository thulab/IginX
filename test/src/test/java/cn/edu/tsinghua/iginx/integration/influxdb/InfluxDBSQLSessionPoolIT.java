package cn.edu.tsinghua.iginx.integration.influxdb;

import cn.edu.tsinghua.iginx.integration.SQLSessionPoolIT;

public class InfluxDBSQLSessionPoolIT extends SQLSessionPoolIT {
    public InfluxDBSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = false;
        this.isAbleToShowTimeSeries = false;
    }
}
