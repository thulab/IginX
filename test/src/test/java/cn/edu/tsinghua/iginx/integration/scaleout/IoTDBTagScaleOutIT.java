package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.TagIT;
import org.junit.Test;

public class IoTDBTagScaleOutIT extends TagIT implements IoTDBBaseScaleOutIT{

    public IoTDBTagScaleOutIT() {
        super();
    }

    @Test
    public void iotdb11_IT() throws Exception {
        TagIT.ifClearData = false;
        this.storageEngineType = "iotdb11";
        TagIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_IT() throws Exception {
        TagIT.ifClearData = false;
        this.storageEngineType = "iotdb12";
        TagIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }
}
