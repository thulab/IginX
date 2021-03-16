package cn.edu.tsinghua.iginx.metadatav2;

import cn.edu.tsinghua.iginx.metadatav2.entity.StorageEngineMeta;

/**
 * Created on 16/03/2021.
 * Description:
 *
 * @author iznauy
 */
public interface StorageEngineChangeHook {

    void onChanged(StorageEngineMeta before, StorageEngineMeta after);

}
