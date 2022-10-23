package cn.edu.tsinghua.iginx.integration.iotdb;

import cn.edu.tsinghua.iginx.integration.BaseSessionPoolIT;

import java.util.LinkedHashMap;

public class IoTDB11SessionPoolIT extends BaseSessionPoolIT {

    public IoTDB11SessionPoolIT() {
        super();
        this.defaultPort2 = 6668;
        this.isAbleToDelete = true;
        this.storageEngineType = "iotdb11";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");
    }
}
