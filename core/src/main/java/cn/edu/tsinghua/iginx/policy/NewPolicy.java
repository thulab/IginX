package cn.edu.tsinghua.iginx.policy;


import cn.edu.tsinghua.iginx.core.processor.PostQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.StorageEngineChangeHook;

public class NewPolicy implements IPolicy {

    protected boolean needReAllocate = false;
    private IPlanSplitter iPlanSplitter;

    @Override
    public PostQueryExecuteProcessor getPostQueryExecuteProcessor() {
        return null;
    }

    @Override
    public PostQueryPlanProcessor getPostQueryPlanProcessor() {
        return null;
    }

    @Override
    public PostQueryProcessor getPostQueryProcessor() {
        return null;
    }

    @Override
    public PostQueryResultCombineProcessor getPostQueryResultCombineProcessor() {
        return null;
    }

    @Override
    public PreQueryExecuteProcessor getPreQueryExecuteProcessor() {
        return null;
    }

    @Override
    public PreQueryPlanProcessor getPreQueryPlanProcessor() {
        return null;
    }

    @Override
    public PreQueryResultCombineProcessor getPreQueryResultCombineProcessor() {
        return null;
    }

    @Override
    public IPlanSplitter getIPlanSplitter() {
        return this.iPlanSplitter;
    }

    @Override
    public void init(IMetaManager iMetaManager) {
        this.iPlanSplitter = new NewPlanSplitter(this, iMetaManager);
        StorageEngineChangeHook hook = getStorageEngineChangeHook();
        if (hook != null) {
            iMetaManager.registerStorageEngineChangeHook(hook);
        }
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            if (before == null && after != null) {
                needReAllocate = true;
            }
        };
    }

    public boolean isNeedReAllocate() {
        return needReAllocate;
    }


    public void setNeedReAllocate(boolean needReAllocate) {
        this.needReAllocate = needReAllocate;
    }
}