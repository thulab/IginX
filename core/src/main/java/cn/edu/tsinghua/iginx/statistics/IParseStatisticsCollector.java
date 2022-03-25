package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostParseProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreParseProcessor;

public interface IParseStatisticsCollector {

  PreParseProcessor getPreParseProcessor();

  PostParseProcessor getPostParseProcessor();

}
