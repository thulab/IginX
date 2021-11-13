package cn.edu.tsinghua.iginx.integration;

import java.util.LinkedHashMap;

public class IoTDB12SessionIT extends BaseSessionIT {

    public IoTDB12SessionIT() {
        super();
        this.defaultPort2 = 6668;
        this.isAbleToDelete = true;
        this.isFromZero = false;
        this.storageEngineType = "iotdb12";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");
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

