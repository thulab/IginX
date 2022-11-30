package cn.edu.tsinghua.iginx.protocol.etcd;

import cn.edu.tsinghua.iginx.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.proposal.SyncVote;
import cn.edu.tsinghua.iginx.proposal.VoteListener;
import cn.edu.tsinghua.iginx.protocol.ExecutionException;
import cn.edu.tsinghua.iginx.protocol.NetworkException;
import cn.edu.tsinghua.iginx.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.protocol.VoteExpiredException;
import cn.edu.tsinghua.iginx.protocol.zk.ZooKeeperSyncProtocolImpl;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ETCDSyncProtocolImpl implements SyncProtocol {

    private static final long MAX_NETWORK_LATENCY = 30000;

    private static final long MAX_LOCK_TIME = 30; // 最长锁住 30 秒

    private static final Logger logger = LoggerFactory.getLogger(ETCDSyncProtocolImpl.class);

    private static final String PATH_SEPARATOR = "/";

    private static final String PROTOCOL_PREFIX = "/protocol";

    private static final String PROTOCOL_LOCK = "/lock" + PROTOCOL_PREFIX;

    private static final String PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE = PROTOCOL_PREFIX + "/%s";

    private static final String PROTOCOL_PROPOSAL_TEMPLATE = PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE + "/%s";

    private static final String PROTOCOL_PROPOSAL_LOCK_TEMPLATE = PROTOCOL_LOCK  + "/%s/%s";

    private static final String VOTE_PREFIX = "/vote";

    private static final String VOTE_PROPOSAL_CONTAINER_TEMPLATE = VOTE_PREFIX + "/%s";

    private static final String VOTE_PROPOSAL_TEMPLATE = VOTE_PROPOSAL_CONTAINER_TEMPLATE + "/%s";

    private final Client client;

    private final String category;

    private Watch.Watcher proposalWatcher = null;

    private Watch.Watcher voteWatcher = null;

    private final Map<String, Long> latestProposalTimes;

    private final Map<String, VoteListener> voteListeners;

    private final ReadWriteLock proposalLock;

    private ProposalListener listener;

    public ETCDSyncProtocolImpl(String category, Client client) {
        this(category, client, null);
    }

    public ETCDSyncProtocolImpl(String category, Client client, ProposalListener listener) {
        this.category = category;
        this.client = client;

        this.listener = listener;
        this.voteListeners = new HashMap<>();
        this.proposalLock = new ReentrantReadWriteLock();
        this.latestProposalTimes = new HashMap<>();

        this.registerProposalListener();
        this.registerGlobalVoteListener();
    }

    private void registerProposalListener() {
        this.proposalWatcher = client.getWatchClient().watch(ByteSequence.from(String.format(PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE, category).getBytes()),
                WatchOption.newBuilder().withPrefix(ByteSequence.from(String.format(PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE, category).getBytes())).build(),
                new Watch.Listener() {

                    @Override
                    public void onNext(WatchResponse watchResponse) {
                        if (ETCDSyncProtocolImpl.this.listener == null) {
                            return;
                        }
                        for (WatchEvent event: watchResponse.getEvents()) {
                            String[] parts = new String(event.getKeyValue().getKey().getBytes()).split(PATH_SEPARATOR);
                            String key = parts[3];
                            long createTime = Long.parseLong(parts[4].split("_")[1]);
                            switch (event.getEventType()) {
                                case PUT:
                                    boolean isCreate = event.getPrevKV().getVersion() == 0;
                                    proposalLock.writeLock().lock();
                                    if (isCreate) {
                                        ETCDSyncProtocolImpl.this.latestProposalTimes.put(key, createTime);
                                    } else {
                                        ETCDSyncProtocolImpl.this.latestProposalTimes.remove(key);
                                    }
                                    proposalLock.writeLock().unlock();
                                    if (isCreate) {
                                        SyncProposal newSyncProposal = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), SyncProposal.class);
                                        ETCDSyncProtocolImpl.this.listener.onCreate(key, newSyncProposal);
                                    } else {
                                        SyncProposal afterSyncProposal = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), SyncProposal.class);
                                        SyncProposal beforeSyncProposal = JsonUtils.fromJson(event.getPrevKV().getValue().getBytes(), SyncProposal.class);
                                        ETCDSyncProtocolImpl.this.listener.onUpdate(key, beforeSyncProposal, afterSyncProposal);
                                    }
                                    break;
                                case DELETE:
                                    break;
                                default:
                                    logger.error("unexpected watchEvent: " + event.getEventType());
                                    break;
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
    }

    private void registerGlobalVoteListener() {
        this.voteWatcher = client.getWatchClient().watch(ByteSequence.from(String.format(VOTE_PROPOSAL_CONTAINER_TEMPLATE, category).getBytes()),
                WatchOption.newBuilder().withPrefix(ByteSequence.from(String.format(VOTE_PROPOSAL_CONTAINER_TEMPLATE, category).getBytes())).withPrevKV(true).build(),
                new Watch.Listener() {
                    @Override
                    public void onNext(WatchResponse watchResponse) {
                        for (WatchEvent event: watchResponse.getEvents()) {
                            String key = new String(event.getKeyValue().getKey().getBytes()).split(PATH_SEPARATOR)[3];
                            switch (event.getEventType()) {
                                case PUT:
                                    if (event.getPrevKV().getVersion() != 0) { // update, unexpected
                                        logger.error("unexpected update for vote");
                                        break;
                                    }
                                    SyncVote vote = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), SyncVote.class);
                                    proposalLock.readLock().lock();
                                    VoteListener voteLister = voteListeners.get(key);
                                    proposalLock.readLock().unlock();
                                    if (voteLister != null) {
                                        voteLister.receive(key, vote);
                                    }
                                    break;
                                default:
                                    logger.error("unexpected watchEvent: " + event.getEventType());
                                    break;
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
    }

    @Override
    public boolean startProposal(String key, SyncProposal syncProposal, VoteListener listener) throws NetworkException {
        long createTime = System.currentTimeMillis();
        String lockPath = String.format(PROTOCOL_PROPOSAL_LOCK_TEMPLATE, this.category, key);
        long leaseId = -1;
        try {
            // lock proposal + category
            leaseId = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(lockPath.getBytes()), leaseId);

            GetResponse response = client.getKVClient().get(ByteSequence.from(String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key).getBytes()),
                    GetOption.newBuilder().withSortOrder(GetOption.SortOrder.DESCEND).withLimit(1L).build()).get();
            if (response.getCount() >= 1L) {
                long lastCreateTime = Long.parseLong(new String(response.getKvs().get(0).getKey().getBytes()).split("_")[1]);
                if (lastCreateTime + MAX_NETWORK_LATENCY > createTime) {
                    logger.warn("start protocol for " + category + "-" + key + " failure, due to repeated request");
                    return false;
                }
            }
            proposalLock.writeLock().lock();
            latestProposalTimes.put(key, createTime);
            voteListeners.put(key, listener);
            proposalLock.writeLock().unlock();

            syncProposal.setCreateTime(createTime);
            client.getKVClient().put(ByteSequence.from((String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key) + PATH_SEPARATOR + "proposal_" + createTime).getBytes()), ByteSequence.from(JsonUtils.toJson(syncProposal))).get();
            return true;
        } catch (Exception e) {
            logger.error("start proposal failure: ", e);
            throw new NetworkException("start proposal failure: ", e);
        } finally {
            if (leaseId != -1L) {
                try {
                    // release proposal + category
                    client.getLockClient().unlock(ByteSequence.from(lockPath.getBytes())).get();
                    client.getLeaseClient().revoke(leaseId).get();
                } catch (Exception e) {
                    logger.error("release lock failure: ", e);
                }
            }

        }
    }

    @Override
    public void registerProposalListener(ProposalListener listener) {
        this.listener = listener;
    }

    @Override
    public void voteFor(String key, SyncVote vote) throws NetworkException, VoteExpiredException {
        logger.info("vote for " + key + " from " + vote.getVoter());
        long voter = vote.getVoter();
        try {
            long createTime = 0L;
            proposalLock.readLock().lock();
            createTime = latestProposalTimes.getOrDefault(key, 0L);
            proposalLock.readLock().unlock();
            if (createTime == 0) {
                throw new VoteExpiredException("vote for expired proposal: " + key);
            }
            client.getKVClient().put(ByteSequence.from((String.format(VOTE_PROPOSAL_TEMPLATE, this.category, key) +
                    PATH_SEPARATOR + "proposal_" + createTime + PATH_SEPARATOR + "voter_" + voter).getBytes()), ByteSequence.from(JsonUtils.toJson(vote))).get();
        } catch (VoteExpiredException e) {
            logger.error("encounter execute error in vote: ", e);
            throw e;
        } catch (Exception e) {
            logger.error("vote for " + category + "-" + key + " failure: ", e);
            throw new NetworkException("vote failure: ", e);
        }
    }

    @Override
    public void endProposal(String key, SyncProposal syncProposal) throws NetworkException, ExecutionException {
        long updateTime = System.currentTimeMillis();
        String lockPath = String.format(PROTOCOL_PROPOSAL_LOCK_TEMPLATE, this.category, key);
        long leaseId = -1;
        try {
            // lock proposal + category
            leaseId = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(lockPath.getBytes()), leaseId);

            GetResponse response = client.getKVClient().get(ByteSequence.from(String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key).getBytes()),
                    GetOption.newBuilder().withSortOrder(GetOption.SortOrder.DESCEND).withLimit(1L).build()).get();
            if (response.getCount() == 0) {
                throw new ExecutionException("can't find proposal for " + key);
            }
            long createTime = Long.parseLong(new String(response.getKvs().get(0).getKey().getBytes()).split("_")[1]);
            syncProposal.setCreateTime(createTime);
            syncProposal.setUpdateTime(updateTime);
            client.getKVClient().put(ByteSequence.from((String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key) + PATH_SEPARATOR + "proposal_" + createTime).getBytes()),
                    ByteSequence.from(JsonUtils.toJson(syncProposal))).get();
            proposalLock.writeLock().lock();
            latestProposalTimes.remove(key);
            voteListeners.remove(key).end(key);
            proposalLock.writeLock().unlock();
        } catch (Exception e) {
            logger.error("end protocol for " + category + "-" + key + " failure: ", e);
            throw new NetworkException("end proposal failure: ", e);
        } finally {
            if (leaseId != -1L) {
                try {
                    // release proposal + category
                    client.getLockClient().unlock(ByteSequence.from(lockPath.getBytes())).get();
                    client.getLeaseClient().revoke(leaseId).get();
                } catch (Exception e) {
                    logger.error("release lock failure: ", e);
                }
            }

        }
    }

    @Override
    public void close(){
        this.proposalWatcher.close();
        this.voteWatcher.close();
    }
}
