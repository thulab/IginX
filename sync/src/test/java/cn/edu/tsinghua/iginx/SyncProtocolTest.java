package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.proposal.SyncVote;
import cn.edu.tsinghua.iginx.proposal.VoteListener;
import cn.edu.tsinghua.iginx.protocol.SyncProtocol;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public abstract class SyncProtocolTest {

    protected abstract SyncProtocol newSyncProtocol(String category);

    @Test(timeout = 10000)
    public void testSingleNodeSingleDecision() throws Exception {
        String category = RandomUtils.randomString(10);
        String key = RandomUtils.randomString(5);
        SyncProtocol protocol = newSyncProtocol(category);

        long id = 1L;

        byte[] initialContent = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] finalContent = "world".getBytes(StandardCharsets.UTF_8);
        byte[] voteContent = "vote".getBytes(StandardCharsets.UTF_8);

        CountDownLatch latch = new CountDownLatch(1);

        protocol.registerProposalListener(new ProposalListener() {
            @Override
            public void onCreate(String proposalKey, SyncProposal syncProposal) {
                assertEquals(key, proposalKey);
                assertArrayEquals(initialContent, syncProposal.getContent());
                assertEquals(id, syncProposal.getProposer());

                // vote for itself
                try {
                    protocol.voteFor(proposalKey, new SyncVote(id, voteContent));
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("unexpected vote failure");
                }
            }

            @Override
            public void onUpdate(String proposalKey, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal) {
                assertEquals(key, proposalKey);
                assertArrayEquals(initialContent, beforeSyncProposal.getContent());
                assertArrayEquals(finalContent, afterSyncProposal.getContent());
                assertEquals(id, beforeSyncProposal.getProposer());
                assertEquals(id, afterSyncProposal.getProposer());

                latch.countDown();

            }

        });

        SyncProposal proposal = new SyncProposal(id, initialContent);
        assertTrue("start proposal failure", protocol.startProposal(key, proposal, new VoteListener() {
            @Override
            public void receive(String voteKey, SyncVote vote) {
                // 确定只能收到来自自己的投票
                assertEquals(key, voteKey);
                assertEquals(id, vote.getVoter());
                assertArrayEquals(voteContent, vote.getContent());

                proposal.setContent(finalContent);
                try {
                    protocol.endProposal(voteKey, proposal);
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("unexpected end proposal failure");
                }
            }

            @Override
            public void end(String key) {

            }
        }));

        latch.await();
        protocol.close();
    }

    @Test(timeout = 10000)
    public void testTwoNodeSingleDecision() throws Exception {
        String category = RandomUtils.randomString(10);
        String key = RandomUtils.randomString(5);

        CountDownLatch latch = new CountDownLatch(2);

        byte[] initialContent = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] finalContent = "world".getBytes(StandardCharsets.UTF_8);

        Map<Long, byte[]> voteContents = new ConcurrentHashMap<>();
        voteContents.put(1L, "vote1".getBytes(StandardCharsets.UTF_8));
        voteContents.put(2L, "vote2".getBytes(StandardCharsets.UTF_8));

        class DecisionThread extends Thread {

            private final long id;

            public DecisionThread(long id) {
                this.id = id;
            }

            @Override
            public void run() {
                try {
                    SyncProtocol protocol = newSyncProtocol(category);
                    protocol.registerProposalListener(new ProposalListener() {
                        @Override
                        public void onCreate(String proposalKey, SyncProposal syncProposal) {
                            assertEquals(key, proposalKey);
                            assertArrayEquals(initialContent, syncProposal.getContent());

                            // vote for proposal
                            try {
                                protocol.voteFor(proposalKey, new SyncVote(id, voteContents.get(id)));
                            } catch (Exception e) {
                                e.printStackTrace();
                                fail("unexpected vote failure");
                            }
                        }

                        @Override
                        public void onUpdate(String proposalKey, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal) {
                            assertEquals(key, proposalKey);
                            assertArrayEquals(initialContent, beforeSyncProposal.getContent());
                            assertArrayEquals(finalContent, afterSyncProposal.getContent());
                            assertEquals(afterSyncProposal.getProposer(), beforeSyncProposal.getProposer());

                            latch.countDown();
                        }

                    });

                    SyncProposal proposal = new SyncProposal(id, initialContent);
                    if (protocol.startProposal(key, proposal, new VoteListener() {

                        private final Set<Long> set = new HashSet<>();

                        private final Lock lock = new ReentrantLock();

                        @Override
                        public void receive(String voteKey, SyncVote vote) {
                            assertEquals(key, voteKey);
                            assertArrayEquals(voteContents.get(vote.getVoter()), vote.getContent());

                            lock.lock();
                            set.add(vote.getVoter());
                            try {
                                if (set.size() == 2) {
                                    proposal.setContent(finalContent);
                                    protocol.endProposal(key, proposal);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                fail();
                            } finally {
                                lock.unlock();
                            }
                        }

                        @Override
                        public void end(String key) {
                            System.out.println("current timestamp: " + System.currentTimeMillis());
                            System.out.println("end vote for " + key);
                        }
                    })) {
                        System.out.println("start protocol success");
                    }

                    latch.await();
                    protocol.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("unexpected exception");
                }

            }
        }

        DecisionThread threadA = new DecisionThread(1L);
        DecisionThread threadB = new DecisionThread(2L);
        threadA.start();
        threadB.start();
        threadA.join();
        threadB.join();

    }

    @Test(timeout = 20000)
    public void testMultiNodeSingleDecision() throws Exception {
        for (int c = 0; c < 10; c++) { // 随机 3-6个节点，测试10次
            int N = RandomUtils.randomNumber(2, 4);

            String category = RandomUtils.randomString(10);
            String key = RandomUtils.randomString(5);

            CountDownLatch latch = new CountDownLatch(N);

            byte[] initialContent = RandomUtils.randomString(5).getBytes(StandardCharsets.UTF_8);
            byte[] finalContent = RandomUtils.randomString(5).getBytes(StandardCharsets.UTF_8);

            Map<Long, byte[]> voteContents = new ConcurrentHashMap<>();
            for (long i = 1L; i <= (long)N; i++) {
                voteContents.put(i, String.format("vote%2d", i).getBytes(StandardCharsets.UTF_8));
            }

            class DecisionThread extends Thread {

                private final long id;

                public DecisionThread(long id) {
                    this.id = id;
                }

                @Override
                public void run() {
                    try {
                        SyncProtocol protocol = newSyncProtocol(category);
                        protocol.registerProposalListener(new ProposalListener() {
                            @Override
                            public void onCreate(String proposalKey, SyncProposal syncProposal) {
                                assertEquals(key, proposalKey);
                                assertArrayEquals(initialContent, syncProposal.getContent());

                                // vote for proposal
                                try {
                                    protocol.voteFor(proposalKey, new SyncVote(id, voteContents.get(id)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    fail("unexpected vote failure");
                                }
                            }

                            @Override
                            public void onUpdate(String proposalKey, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal) {
                                assertEquals(key, proposalKey);
                                assertArrayEquals(initialContent, beforeSyncProposal.getContent());
                                assertArrayEquals(finalContent, afterSyncProposal.getContent());
                                assertEquals(afterSyncProposal.getProposer(), beforeSyncProposal.getProposer());

                                latch.countDown();
                            }

                        });

                        SyncProposal proposal = new SyncProposal(id, initialContent);
                        if (protocol.startProposal(key, proposal, new VoteListener() {

                            private final Set<Long> set = new HashSet<>();

                            private final Lock lock = new ReentrantLock();

                            @Override
                            public void receive(String voteKey, SyncVote vote) {
                                assertEquals(key, voteKey);
                                assertArrayEquals(voteContents.get(vote.getVoter()), vote.getContent());

                                lock.lock();
                                set.add(vote.getVoter());
                                try {
                                    if (set.size() == N) {
                                        proposal.setContent(finalContent);
                                        protocol.endProposal(key, proposal);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    fail();
                                } finally {
                                    lock.unlock();
                                }
                            }

                            @Override
                            public void end(String key) {
                                System.out.println("current timestamp: " + System.currentTimeMillis());
                                System.out.println("end vote for " + key);
                            }
                        })) {
                            System.out.println("start protocol success");
                        }

                        latch.await();
                        protocol.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception");
                    }

                }
            }

            Thread[] threads = new Thread[N];
            for (int i = 0; i < N; i++) {
                threads[i] = new DecisionThread(i + 1);
            }
            for (int i = 0; i < N; i++) {
                threads[i].start();
            }
            for (int i = 0; i < N; i++) {
                threads[i].join();
            }
        }
    }

    @Test(timeout = 20000)
    public void testMultiNodeSingleDecisionPartialVote() throws Exception {
        for (int c = 0; c < 10; c++) { // 随机 2-4 个节点，测试10次
            int N = RandomUtils.randomNumber(2, 5);
            int M = RandomUtils.randomNumber(0, N);
            String category = RandomUtils.randomString(10);
            String key = RandomUtils.randomString(5);

            CountDownLatch latch = new CountDownLatch(N);
            byte[] initialContent = RandomUtils.randomString(5).getBytes(StandardCharsets.UTF_8);
            byte[] finalSuccessContent = RandomUtils.randomString(5).getBytes(StandardCharsets.UTF_8);
            byte[] finalFailureContent = RandomUtils.randomString(5).getBytes(StandardCharsets.UTF_8);

            Map<Long, byte[]> voteContents = new ConcurrentHashMap<>();
            for (long i = 1L; i <= (long)N; i++) {
                if (i - 1 == M || RandomUtils.randomTest(M * 1.0 / N)) {
                    voteContents.put(i, String.format("vote%2d", i).getBytes(StandardCharsets.UTF_8));
                }
            }

            class DecisionThread extends Thread {

                private final long id;

                public DecisionThread(long id) {
                    this.id = id;
                }

                @Override
                public void run() {
                    try {
                        SyncProtocol protocol = newSyncProtocol(category);
                        protocol.registerProposalListener(new ProposalListener() {
                            @Override
                            public void onCreate(String proposalKey, SyncProposal syncProposal) {
                                assertEquals(key, proposalKey);
                                assertArrayEquals(initialContent, syncProposal.getContent());

                                // vote for proposal
                                byte[] voteContent = voteContents.get(id);
                                if (voteContent == null) {
                                    return;
                                }
                                try {
                                    protocol.voteFor(proposalKey, new SyncVote(id, voteContent));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    fail("unexpected vote failure");
                                }
                            }

                            @Override
                            public void onUpdate(String proposalKey, SyncProposal beforeSyncProposal, SyncProposal afterSyncProposal) {
                                assertEquals(key, proposalKey);
                                assertArrayEquals(initialContent, beforeSyncProposal.getContent());
                                if (voteContents.size() >= (N + 1) / 2) {
                                    assertArrayEquals(finalSuccessContent, afterSyncProposal.getContent());
                                } else {
                                    assertArrayEquals(finalFailureContent, afterSyncProposal.getContent());
                                }
                                assertEquals(afterSyncProposal.getProposer(), beforeSyncProposal.getProposer());

                                latch.countDown();
                            }

                        });

                        SyncProposal proposal = new SyncProposal(id, initialContent);
                        if (protocol.startProposal(key, proposal, new VoteListener() {

                            private final Set<Long> set = new HashSet<>();

                            private final Lock lock = new ReentrantLock();

                            @Override
                            public void receive(String voteKey, SyncVote vote) {
                                assertEquals(key, voteKey);
                                assertArrayEquals(voteContents.get(vote.getVoter()), vote.getContent());

                                lock.lock();
                                set.add(vote.getVoter());
                                try {
                                    if (set.size() == voteContents.size()) { // 模拟收到的票不够的情况
                                        if (voteContents.size() >= (N + 1) / 2) {
                                            proposal.setContent(finalSuccessContent);
                                        } else {
                                            proposal.setContent(finalFailureContent);
                                        }
                                        protocol.endProposal(key, proposal);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    fail();
                                } finally {
                                    lock.unlock();
                                }
                            }

                            @Override
                            public void end(String key) {
                                System.out.println("current timestamp: " + System.currentTimeMillis());
                                System.out.println("end vote for " + key);
                            }
                        })) {
                            System.out.println("start protocol success");
                        }

                        latch.await();
                        protocol.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception");
                    }

                }
            }

            Thread[] threads = new Thread[N];
            for (int i = 0; i < N; i++) {
                threads[i] = new DecisionThread(i + 1);
            }
            for (int i = 0; i < N; i++) {
                threads[i].start();
            }
            for (int i = 0; i < N; i++) {
                threads[i].join();
            }

        }
    }

}
