package cn.edu.tsinghua.iginx.integration;

import java.util.LinkedHashMap;

public class IoTDB12SessionPoolIT extends BaseSessionPoolIT {

    public IoTDB12SessionPoolIT() {
        super();
        this.defaultPort2 = 6668;
        this.isAbleToDelete = true;
        this.storageEngineType = "iotdb12";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");
    }
}
