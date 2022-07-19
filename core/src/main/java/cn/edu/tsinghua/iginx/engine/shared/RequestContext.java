package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

@Data
public class RequestContext {

    private long id;

    private long startTime;

    private long endTime;

    private long sessionId;

    private Map<String, Object> extraParams;

    private Status status;

    private String sql;

    private boolean fromSQL;

    private SqlType sqlType;

    private Statement statement;

    private Result result;

    private boolean useStream;

    private void init() {
        this.id = SnowFlakeUtils.getInstance().nextId();
        this.startTime = System.currentTimeMillis();
        this.extraParams = new HashMap<>();
    }

    public RequestContext(long sessionId) {
        init();
        this.sessionId = sessionId;
    }

    public RequestContext(long sessionId, Statement statement) {
        this(sessionId, statement, false);
    }

    public RequestContext(long sessionId, Statement statement, boolean useStream) {
        init();
        this.sessionId = sessionId;
        this.statement = statement;
        this.fromSQL = false;
        this.useStream = useStream;
    }

    public RequestContext(long sessionId, String sql) {
        this(sessionId, sql, false);
    }

    public RequestContext(long sessionId, String sql, boolean useStream) {
        init();
        this.sessionId = sessionId;
        this.sql = sql;
        this.fromSQL = true;
        this.sqlType = SqlType.Unknown;
        this.useStream = useStream;
    }

    public Object getExtraParam(String key) {
        return extraParams.getOrDefault(key, null);
    }

    public void setExtraParam(String key, Object value) {
        extraParams.put(key, value);
    }

    public boolean isUseStream() {
        return useStream;
    }

    public void setResult(Result result) {
        this.result = result;
        if (this.result != null) {
            this.result.setQueryId(id);
        }
        this.endTime = System.currentTimeMillis();
    }

    public boolean isFromREST(){
        return fromREST;
    }

    public Map<String, List<String>> getSelectTagList(){
        return this.selectTagList;
    }
}
