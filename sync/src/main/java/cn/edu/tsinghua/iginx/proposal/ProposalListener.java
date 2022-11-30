package cn.edu.tsinghua.iginx.proposal;

public interface ProposalListener {

    /**
     * when proposal created, this method will be called.
     * @param key proposal key
     * @param syncProposal proposal content
     */
    void onCreate(String key, SyncProposal syncProposal);

    /**
     * when proposal updated, this method will be called.
     * @param key proposal key
     * @param beforeSyncProposal proposal content before update
     * @param afterSyncProposal proposal content after update
     */
    void onUpdate(String key, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal);

    /**
     * when proposal deleted, this method will be called.
     * @param key proposal key
     * @param syncProposal proposal content
     */
    void onDelete(String key, SyncProposal syncProposal);

}
