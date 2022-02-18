package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostPhysicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PrePhysicalProcessor;

public interface IPhysicalStatisticsCollector {

    PrePhysicalProcessor getPrePhysicalProcessor();

    PostPhysicalProcessor getPostPhysicalProcessor();

}
