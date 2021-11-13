package cn.edu.tsinghua.iginx.integration;

import java.util.LinkedHashMap;

public class InfluxDBSessionIT extends BaseSessionIT {

    public InfluxDBSessionIT() {
        super();
        this.defaultPort2 = 8087;
        this.isAbleToDelete = false;
        this.isFromZero = true;
        this.storageEngineType = "influxdb";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("url", "http://localhost:8087/");
        this.extraParams.put("token", "testToken");
        this.extraParams.put("organization", "testOrg");
    }

    @Override
    protected void errorOperationTest() {

    }

    @Override
    protected void addStorage(){
        try {
            session.addStorageEngine("127.0.0.1", defaultPort2, storageEngineType, extraParams);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
