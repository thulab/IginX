/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.listener;

import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.proposal.content.LossConnectionProposalContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.proposal.content.RestoreConnectionProposalContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.content.LossConnectionVoteContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.content.RestoreConnectionVoteContent;
import cn.edu.tsinghua.iginx.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.proposal.SyncVote;
import cn.edu.tsinghua.iginx.proposal.VoteListener;
import cn.edu.tsinghua.iginx.protocol.ExecutionException;
import cn.edu.tsinghua.iginx.protocol.NetworkException;
import cn.edu.tsinghua.iginx.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RestoreConnectionVoteListener implements VoteListener {

    private static final Logger logger = LoggerFactory.getLogger(RestoreConnectionVoteListener.class);

    private final int targetVote;

    private final Map<Long, SyncVote> votes = new HashMap<>();

    private final SyncProposal proposal;

    private final SyncProtocol protocol;

    public RestoreConnectionVoteListener(int targetVote, SyncProposal proposal, SyncProtocol protocol) {
        this.targetVote = targetVote;
        this.proposal = proposal;
        this.protocol = protocol;
    }

    @Override
    public synchronized void receive(String key, SyncVote vote) {
        long voter = vote.getVoter();
        votes.put(voter, vote);
        if (votes.size() != targetVote) {
            return;
        }
        int supportCount = 0;
        for (SyncVote v: votes.values()) {
            RestoreConnectionVoteContent content = JsonUtils.fromJson(v.getContent(), RestoreConnectionVoteContent.class);
            if (!content.isAlive()) {
                supportCount++;
            }
        }
        boolean alive = true;
        if (supportCount * 2 > targetVote) {
            alive = false;
        }
        RestoreConnectionProposalContent content = JsonUtils.fromJson(proposal.getContent(), RestoreConnectionProposalContent.class);
        content.setAlive(alive);
        proposal.setContent(JsonUtils.toJson(content));
        try {
            protocol.endProposal(key, proposal);
        } catch (NetworkException | ExecutionException e) {
            logger.error("end proposal failure: ", e);
        }
    }

    @Override
    public void end(String key) {
        logger.info("current timestamp: " + System.currentTimeMillis() + ", end restore connection vote for " + key);
    }
}
