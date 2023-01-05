package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;

import java.util.List;

public class Migration extends AbstractUnaryOperator {

    private final FragmentMeta fragmentMeta;
    private final List<String> paths;
    private final StorageUnitMeta targetStorageUnitMeta;

    public Migration(GlobalSource source, FragmentMeta fragmentMeta,
                     List<String> paths, StorageUnitMeta targetStorageUnitMeta) {
        super(OperatorType.Migration, source);
        this.fragmentMeta = fragmentMeta;
        this.paths = paths;
        this.targetStorageUnitMeta = targetStorageUnitMeta;
    }

    public FragmentMeta getFragmentMeta() {
        return fragmentMeta;
    }

    public StorageUnitMeta getTargetStorageUnitMeta() {
        return targetStorageUnitMeta;
    }

    public List<String> getPaths() {
        return paths;
    }

    @Override
    public Operator copy() {
        return new Migration((GlobalSource) getSource().copy(), fragmentMeta, paths, targetStorageUnitMeta);
    }
}
