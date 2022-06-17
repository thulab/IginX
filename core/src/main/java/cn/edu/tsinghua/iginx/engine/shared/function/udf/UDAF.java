package cn.edu.tsinghua.iginx.engine.shared.function.udf;

import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;

public interface UDAF extends SetMappingFunction {

    String getFunctionName();

}
