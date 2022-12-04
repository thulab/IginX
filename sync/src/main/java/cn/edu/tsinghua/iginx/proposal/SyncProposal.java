package cn.edu.tsinghua.iginx.proposal;

public class SyncProposal {

    private long createTime;

    private long updateTime;

    private final long proposer;

    private byte[] content;

    public SyncProposal(long proposer, byte[] content) {
        this.proposer = proposer;
        this.content = content;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public long getProposer() {
        return proposer;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

}
