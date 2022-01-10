package cn.edu.tsinghua.iginx.policy.sampler;

import java.util.List;

public interface Sampler {

    void updatePrefix(List<String> paths);

    List<String> samplePrefix(int count);
}
