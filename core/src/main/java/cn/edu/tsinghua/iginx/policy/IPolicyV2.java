package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;

public interface IPolicyV2 {

    void notify(DataStatement statement);

    IFragmentGeneratorV2 getIFragmentGenerator();

    void init(IMetaManager iMetaManager);

    StorageEngineChangeHook getStorageEngineChangeHook();

    boolean isNeedReAllocate();

    void setNeedReAllocate(boolean needReAllocate);
}
