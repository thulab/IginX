package cn.edu.tsinghua.iginx.metadata.hook;

public interface VersionChangeHook {

  void onChange(int version, int num);

}
