package cn.edu.tsinghua.iginx.protocol;

import cn.edu.tsinghua.iginx.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.proposal.SyncVote;
import cn.edu.tsinghua.iginx.proposal.VoteListener;

public interface SyncProtocol {

    boolean startProposal(String key, SyncProposal syncProposal, VoteListener listener) throws NetworkException;

    void registerProposalListener(ProposalListener listener) ;

    void voteFor(String key, SyncVote vote) throws NetworkException, VoteExpiredException;

    void endProposal(String key, SyncProposal syncProposal) throws NetworkException, ExecutionException;

}
