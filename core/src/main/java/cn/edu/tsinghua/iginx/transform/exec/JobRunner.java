package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.UnknownDataFlowException;
import cn.edu.tsinghua.iginx.transform.pojo.BatchStage;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.StreamStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JobRunner implements Runner {

    private final Job job;

    private final List<Runner> runnerList;

    private final static Logger logger = LoggerFactory.getLogger(JobRunner.class);

    public JobRunner(Job job) {
        this.job = job;
        this.runnerList = new ArrayList<>();
    }

    @Override
    public void start() throws UnknownDataFlowException {
        for (Stage stage : job.getStageList()) {
            DataFlowType dataFlowType = stage.getStageType();
            switch (dataFlowType) {
                case Batch:
                    runnerList.add(new BatchStageRunner((BatchStage) stage));
                    break;
                case Stream:
                    runnerList.add(new StreamStageRunner((StreamStage) stage));
                    break;
                default:
                    logger.error(String.format("Unknown stage type %s", dataFlowType.toString()));
                    throw new UnknownDataFlowException(dataFlowType);
            }
        }
    }

    @Override
    public void run() {
        job.setState(JobState.JOB_RUNNING);
        try {
            for (Runner runner: runnerList) {
                runner.start();
                runner.run();
                runner.close();
            }
            job.setState(JobState.JOB_FINISHED);
        } catch (TransformException e) {
            logger.error(String.format("Fail to run transform job id=%d, because", job.getJobId()), e);
            job.setState(JobState.JOB_FAILING);
            close();
            job.setState(JobState.JOB_FAILED);
        }
    }

    @Override
    public void close() {
        try {
            for (Runner runner: runnerList) {
                runner.close();
            }
        } catch (TransformException e) {
            logger.error(String.format("Fail to close Transform job runner id=%d, because", job.getJobId()), e);
        }
    }
}
