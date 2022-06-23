package cn.edu.tsinghua.iginx.transform.api;

import cn.edu.tsinghua.iginx.transform.driver.IPCWorker;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;

public interface Driver {

    IPCWorker createWorker(PythonTask task, Writer writer) throws TransformException;
}
