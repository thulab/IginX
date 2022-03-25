package cn.edu.tsinghua.iginx.engine.shared.processor;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.thrift.Status;

public interface Processor {

  Status process(RequestContext requestContext);

}
