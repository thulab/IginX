package cn.edu.tsinghua.iginx.proposal;

public class SyncVote {

    private final long voter;

    private final byte[] content;

    public SyncVote(long voter, byte[] content) {
        this.voter = voter;
        this.content = content;
    }

    public long getVoter() {
        return voter;
    }

    public byte[] getContent() {
        return content;
    }
}
