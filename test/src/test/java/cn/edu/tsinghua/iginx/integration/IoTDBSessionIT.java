package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;

import java.util.LinkedHashMap;

public class IoTDBSessionIT extends BaseSessionIT {

    public IoTDBSessionIT(){
        super();
        this.defaultPort2 = 6668;
        this.isAbleForDelete = true;
        this.storageEngineType = StorageEngineType.IOTDB;
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");
    }
}
