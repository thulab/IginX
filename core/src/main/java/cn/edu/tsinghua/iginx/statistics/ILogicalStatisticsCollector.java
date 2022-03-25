package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostLogicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreLogicalProcessor;

public interface ILogicalStatisticsCollector {

  PreLogicalProcessor getPreLogicalProcessor();

  PostLogicalProcessor getPostLogicalProcessor();

}
