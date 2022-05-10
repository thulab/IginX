package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.domain.Transform;
import cn.edu.tsinghua.iginx.thrift.JobState;

public interface TransformClient {

    long commitTransformJob(final Transform transform);

    JobState queryTransformJobStatus(long jobId);

    void cancelTransformJob(long jobId);

}
