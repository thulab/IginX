package cn.edu.tsinghua.iginx.transaction.context;

public class TransactionRecord {

    private String id;

    private long timestamp;

    private TransactionState state;

    public TransactionRecord(String id, long timestamp, TransactionState state) {
        this.id = id;
        this.timestamp = timestamp;
        this.state = state;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) {
        this.state = state;
    }
}
