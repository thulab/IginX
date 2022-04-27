package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.thrift.CommitTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Checker;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.*;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransformJobManager {

    private final Map<Long, Job> jobMap;

    private final Map<Long, JobRunner> jobRunnerMap;

    private static TransformJobManager instance;

    private final ExecutorService threadPool;

    private final Checker checker = JobValidationChecker.getInstance();

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private final static Logger logger = LoggerFactory.getLogger(TransformJobManager.class);

    private TransformJobManager() {
        this.jobMap = new ConcurrentHashMap<>();
        this.jobRunnerMap = new ConcurrentHashMap<>();
        this.threadPool = Executors.newFixedThreadPool(config.getTransformTaskThreadPoolSize());
    }

    public static TransformJobManager getInstance() {
        if (instance == null) {
            synchronized (TransformJobManager.class) {
                if (instance == null) {
                    instance = new TransformJobManager();
                }
            }
        }
        return instance;
    }

    public long commit(CommitTransformJobReq jobReq) {
        long jobId = SnowFlakeUtils.getInstance().nextId();
        Job job = new Job(jobId, jobReq);
        return commitJob(job);
    }

    public long commitJob(Job job) {
        if (checker.check(job)) {
            jobMap.put(job.getJobId(), job);
            threadPool.submit(() -> process(job));
            return job.getJobId();
        } else {
            logger.error("Committed job is illegal.");
            return -1;
        }
    }

    private void process(Job job) {
        JobRunner runner = new JobRunner(job);
        job.setStartTime(System.currentTimeMillis());
        try {
            runner.start();
            jobRunnerMap.put(job.getJobId(), runner);
            runner.run();
            jobRunnerMap.remove(job.getJobId());
        } catch (Exception e) {
            logger.error(String.format("Fail to process transform job id=%d", job.getJobId()));
            e.printStackTrace();
        } finally {
            runner.close();
            job.setEndTime(System.currentTimeMillis());
        }
    }

    public void cancel(long jobId) {
        Job job = jobMap.get(jobId);
        job.setState(JobState.JOB_CLOSING);

        JobRunner runner = jobRunnerMap.get(jobId);
        runner.close();
        jobRunnerMap.remove(jobId);

        job.setEndTime(System.currentTimeMillis());
        job.setState(JobState.JOB_CLOSED);
    }

    public JobState queryJobState(long jobId) {
        if (jobMap.containsKey(jobId)) {
            return jobMap.get(jobId).getState();
        } else {
            return null;
        }
    }
}
