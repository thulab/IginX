package cn.edu.tsinghua.iginx.proposal;

public interface VoteListener {

    void receive(String key, SyncVote vote);

    void end(String key);

}
