package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;

import java.util.LinkedHashMap;

public class IoTDBBaseSessionCapExpIT extends BaseSessionIT {
    public IoTDBBaseSessionCapExpIT() {
        super();
    }

    @Override
    public void iotdb11_IT() {
        this.defaultPort2 = 6668;
        this.isAbleToDelete = true;
        this.storageEngineType = "iotdb11";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");

        this.ifClearData = false;
        try {
            BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            capacityExpansion();
        } catch (InterruptedException | ExecutionException | SessionException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void iotdb12_IT() {
        this.defaultPort2 = 6668;
        this.isAbleToDelete = true;
        this.storageEngineType = "iotdb12";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");

        this.ifClearData = false;
        try {
            BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            capacityExpansion();
        } catch (InterruptedException | ExecutionException | SessionException e) {
            logger.error(e.getMessage());
        }
    }

}
