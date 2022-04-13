package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

public class Migration extends AbstractUnaryOperator {

  private final long sourceStorageEngineId;
  private final long targetStorageEngineId;
  private final FragmentMeta fragmentMeta;

  public Migration(GlobalSource source, long sourceStorageEngineId, long targetStorageEngineId,
      FragmentMeta fragmentMeta) {
    super(OperatorType.Migration, source);
    this.sourceStorageEngineId = sourceStorageEngineId;
    this.targetStorageEngineId = targetStorageEngineId;
    this.fragmentMeta = fragmentMeta;
  }

  public long getSourceStorageEngineId() {
    return sourceStorageEngineId;
  }

  public long getTargetStorageEngineId() {
    return targetStorageEngineId;
  }

  public FragmentMeta getFragmentMeta() {
    return fragmentMeta;
  }

  @Override
  public Operator copy() {
    return new Migration((GlobalSource) getSource().copy(), sourceStorageEngineId,
        targetStorageEngineId, fragmentMeta);
  }
}
