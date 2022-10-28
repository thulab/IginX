package cn.edu.tsinghua.iginx.integration.timescaledb;

import cn.edu.tsinghua.iginx.integration.BaseSessionIT;

import java.util.LinkedHashMap;

public class TimescaleDBSessionIT extends BaseSessionIT {

    public TimescaleDBSessionIT() {
        super();
        this.defaultPort2 = 5433;
        this.isAbleToDelete = true;
        this.storageEngineType = "timescaledb";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "postgres");
        this.extraParams.put("password", "123456");
        this.extraParams.put("dbname", "timeseries");
    }
}
