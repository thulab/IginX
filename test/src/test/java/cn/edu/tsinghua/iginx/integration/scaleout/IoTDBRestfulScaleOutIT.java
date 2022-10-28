package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import org.junit.Test;

public class IoTDBRestfulScaleOutIT extends RestIT implements IoTDBBaseScaleOutIT{
    public IoTDBRestfulScaleOutIT() {
        super();
    }

    @Test
    public void iotdb11_IT() throws Exception {
        this.ifClearData = false;
        this.storageEngineType = "iotdb11";
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_IT() throws Exception {
        this.ifClearData = false;
        this.storageEngineType = "iotdb12";
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }
}
