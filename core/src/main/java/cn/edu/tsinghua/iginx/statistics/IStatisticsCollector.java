package cn.edu.tsinghua.iginx.statistics;

public interface IStatisticsCollector extends IParseStatisticsCollector,
    ILogicalStatisticsCollector, IPhysicalStatisticsCollector, IExecuteStatisticsCollector {

  void startBroadcasting();

  void endBroadcasting();

}
