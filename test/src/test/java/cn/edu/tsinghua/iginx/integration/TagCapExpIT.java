package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;

public class TagCapExpIT extends TagIT{

    public TagCapExpIT() {
        super();
    }

    @Override
    public void iotdb11_IT() {
        TagIT.ifClearData = false;
        this.storageEngineType = "iotdb11";
        try {
            TagIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            capacityExpansion();
        } catch (ExecutionException | SessionException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void iotdb12_IT() {
        TagIT.ifClearData = false;
        this.storageEngineType = "iotdb12";
        try {
            TagIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:false\");");
            capacityExpansion();
        } catch (ExecutionException | SessionException e) {
            logger.error(e.getMessage());
        }
    }
}