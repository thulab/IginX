package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.TransformTaskMeta;

public interface TransformChangeHook {

    void onChange(String className, TransformTaskMeta transformTask);

}
