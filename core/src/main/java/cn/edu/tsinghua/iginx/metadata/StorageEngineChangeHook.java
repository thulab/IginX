package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;

/**
 * Created on 16/03/2021.
 * Description:
 *
 * @author iznauy
 */
public interface StorageEngineChangeHook {

    void onChanged(StorageEngineMeta before, StorageEngineMeta after);

}
