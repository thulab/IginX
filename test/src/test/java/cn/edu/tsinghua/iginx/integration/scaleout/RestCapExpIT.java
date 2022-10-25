package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.rest.RestIT;

public class RestCapExpIT extends RestIT {
    public RestCapExpIT() {
        super();
    }

    @Override
    public void iotdb11_IT() {
        this.ifClearData = false;
        this.storageEngineType = "iotdb11";
        try {
            RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            capacityExpansion();
        } catch (Exception e) {
            RestIT.logger.error(e.getMessage());
        }

    }

    @Override
    public void iotdb12_IT() {
        this.ifClearData = false;
        this.storageEngineType = "iotdb12";
        try {
            RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            capacityExpansion();
        } catch (Exception e) {
            RestIT.logger.error(e.getMessage());
        }
    }
}
