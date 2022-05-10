package cn.edu.tsinghua.iginx.transform.api;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.transform.data.ExportWriter;

public interface Stage {

    DataFlowType getStageType();

    ExportWriter getExportWriter();

}
