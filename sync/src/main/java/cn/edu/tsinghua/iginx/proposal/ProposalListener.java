package cn.edu.tsinghua.iginx.proposal;

public interface ProposalListener {

    void onCreate(String key, SyncProposal syncProposal);

    void onUpdate(String key, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal);

    void onDelete(String key, SyncProposal syncProposal);

}
