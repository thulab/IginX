package cn.edu.tsinghua.iginx.transform.utils;

import java.util.HashMap;
import java.util.Map;

public class Constants {

    public static final String KEY = "key";

    public static final String PARAM_PATHS = "param_paths";
    public static final String PARAM_LEVELS = "param_levels";
    public static final String PARAM_MODULE = "param_module";
    public static final String PARAM_CLASS = "param_class";

    public static final String UDF_CLASS = "t";
    public static final String UDF_FUNC = "transform";

    public static final Map<Integer, String> WORKER_STATUS_MAP = new HashMap<>();
    static {
        WORKER_STATUS_MAP.put(0, "SUCCESS");
        WORKER_STATUS_MAP.put(-1, "FAIL_TO_CREATE_SOCKET");
        WORKER_STATUS_MAP.put(-2, "FAIL_TO_BIND_ADDR");
        WORKER_STATUS_MAP.put(-3, "FAIL_TO_LOAD_CLASS");
    }

    public static String getWorkerStatusInfo(int status) {
        return WORKER_STATUS_MAP.get(status);
    }
}
