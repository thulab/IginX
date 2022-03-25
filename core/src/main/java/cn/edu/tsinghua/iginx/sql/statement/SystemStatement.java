package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;

public abstract class SystemStatement extends Statement {

  public abstract void execute(RequestContext ctx) throws ExecutionException;
}
