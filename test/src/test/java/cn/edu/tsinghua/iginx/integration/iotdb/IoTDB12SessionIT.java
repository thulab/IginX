package cn.edu.tsinghua.iginx.integration.iotdb;

import cn.edu.tsinghua.iginx.integration.BaseSessionIT;

import java.util.LinkedHashMap;

public class IoTDB12SessionIT extends BaseSessionIT {

    public IoTDB12SessionIT() {
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

