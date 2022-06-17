package cn.edu.tsinghua.iginx.engine.shared.function.udf;

import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;

public interface UDTF extends RowMappingFunction {

    String getFunctionName();

}
