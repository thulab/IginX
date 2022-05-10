package cn.edu.tsinghua.iginx.transform.api;

import cn.edu.tsinghua.iginx.transform.exception.TransformException;

public interface Runner {

    void start() throws TransformException;

    void run() throws TransformException;

    void close() throws TransformException;
}
