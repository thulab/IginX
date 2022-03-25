package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostExecuteProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreExecuteProcessor;

public interface IExecuteStatisticsCollector {

  PreExecuteProcessor getPreExecuteProcessor();

  PostExecuteProcessor getPostExecuteProcessor();

}
