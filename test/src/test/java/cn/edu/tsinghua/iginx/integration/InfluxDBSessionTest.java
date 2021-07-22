package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;

import java.util.LinkedHashMap;

public class InfluxDBSessionTest extends BaseSessionIT {

    public InfluxDBSessionTest(){
        super();
        this.defaultPort2 = 8087;
        this.isAbleForDelete = false;
        this.storageEngineType = StorageEngineType.INFLUXDB;
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("url", "http://localhost:8087/");
    }
}
