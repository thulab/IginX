package cn.edu.tsinghua.iginx.engine.shared.source;

public class GlobalSource extends AbstractSource {

  public GlobalSource() {
    super(SourceType.Global);
  }

  @Override
  public Source copy() {
    return new GlobalSource();
  }
}
