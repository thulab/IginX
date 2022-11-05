package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.junit.Test;

public class IoTDBRestfulScaleOutIT extends RestIT implements IoTDBBaseScaleOutIT{
    public IoTDBRestfulScaleOutIT() {
        super();
    }

    @Override
    public void DBConf() throws Exception {
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/conf/DBConf.txt");
        this.ifClearData = false;
    }

    @Override
    public void OriHasDataExpHasData_IT() throws Exception {
        DBConf();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Override
    public void OriHasDataExpNoData_IT() throws Exception {
        DBConf();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Override
    public void OriNoDataExpHasData_IT() throws Exception {
        DBConf();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Override
    public void OriNoDataExpNoData_IT() throws Exception {
        DBConf();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }
}
