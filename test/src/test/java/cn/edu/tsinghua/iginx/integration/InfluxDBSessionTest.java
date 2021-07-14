package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;

public class InfluxDBSessionTest extends BaseSessionIT {

    public InfluxDBSessionTest(){
        super();
        this.defaultPort2 = 8087;
        this.isAbleForDelete = false;
        this.storageEngineType = StorageEngineType.INFLUXDB;
    }
}
