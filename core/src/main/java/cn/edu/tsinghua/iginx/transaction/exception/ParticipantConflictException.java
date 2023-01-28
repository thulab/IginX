package cn.edu.tsinghua.iginx.transaction.exception;

public class ParticipantConflictException extends ParticipantException {

    /**
     * 与当前事务冲突的事务 id
     */
    private String conflictId;

    public String getConflictId() {
        return conflictId;
    }

    public void setConflictId(String conflictId) {
        this.conflictId = conflictId;
    }
}
