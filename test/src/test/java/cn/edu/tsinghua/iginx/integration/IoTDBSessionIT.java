package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;

public class IoTDBSessionIT extends BaseSessionIT {

    public IoTDBSessionIT(){
        super();
        this.defaultPort2 = 6668;
        this.isAbleForDelete = true;
        this.storageEngineType = StorageEngineType.IOTDB;
    }
}
