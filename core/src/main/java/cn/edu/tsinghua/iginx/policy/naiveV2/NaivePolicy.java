package cn.edu.tsinghua.iginx.policy.naiveV2;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.IFragmentGeneratorV2;
import cn.edu.tsinghua.iginx.policy.IPolicyV2;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class NaivePolicy implements IPolicyV2 {

    protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
    private IMetaManager iMetaManager;
    private IFragmentGeneratorV2 iFragmentGenerator;

    @Override
    public void notify(DataStatement statement) {
        List<String> pathList;
        switch (statement.getType()) {
            case SELECT:
                pathList = new ArrayList<>(((SelectStatement) statement).getPathSet());
                iFragmentGenerator.getSampler().updatePrefix(new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size() - 1))));
                break;
            case DELETE:
                pathList = ((DeleteStatement) statement).getPaths();
                iFragmentGenerator.getSampler().updatePrefix(new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size() - 1))));
                break;
            case INSERT:
                pathList = ((InsertStatement) statement).getPaths();
                iFragmentGenerator.getSampler().updatePrefix(new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size() - 1))));
                break;
        }
    }

    @Override
    public IFragmentGeneratorV2 getIFragmentGenerator() {
        return iFragmentGenerator;
    }

    @Override
    public void init(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
        this.iFragmentGenerator = new NaiveFragmentGenerator(this.iMetaManager);
        StorageEngineChangeHook hook = getStorageEngineChangeHook();
        if (hook != null) {
            iMetaManager.registerStorageEngineChangeHook(hook);
        }
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            // 哪台机器加了分片，哪台机器初始化，并且在批量添加的时候只有最后一个存储引擎才会导致扩容发生
            if (before == null && after != null && after.getCreatedBy() == iMetaManager.getIginxId() && after.isLastOfBatch()) {
                needReAllocate.set(true);
            }
            // TODO: 针对节点退出的情况缩容
        };
    }

    public boolean isNeedReAllocate() {
        return needReAllocate.getAndSet(false);
    }

    public void setNeedReAllocate(boolean needReAllocate) {
        this.needReAllocate.set(needReAllocate);
    }
}
