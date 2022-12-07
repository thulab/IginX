package cn.edu.tsinghua.iginx.protocol;

import cn.edu.tsinghua.iginx.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.proposal.SyncVote;
import cn.edu.tsinghua.iginx.proposal.VoteListener;

public interface SyncProtocol {

    /**
     * start a sync proposal
     * @param key proposal key
     * @param syncProposal proposal content
     * @param listener vote listener
     * @return success or failure
     */
    boolean startProposal(String key, SyncProposal syncProposal, VoteListener listener) throws NetworkException;

    /**
     * register proposal listener, when proposal create/update/delete, listener will receive notification
     * @param listener proposal listener
     */
    void registerProposalListener(ProposalListener listener) ;

    /**
     * vote for proposal
     * @param key proposal key
     * @param vote proposal vote
     */
    void voteFor(String key, SyncVote vote) throws NetworkException, VoteExpiredException;

    /**
     * end proposal
     * @param key proposal key
     * @param syncProposal proposal content
     */
    void endProposal(String key, SyncProposal syncProposal) throws NetworkException, ExecutionException;

    void close();

}
