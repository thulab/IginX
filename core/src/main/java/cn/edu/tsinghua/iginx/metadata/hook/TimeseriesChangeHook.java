package cn.edu.tsinghua.iginx.metadata.hook;

public interface TimeSeriesChangeHook {

    void onChange(int node, int version);

}
