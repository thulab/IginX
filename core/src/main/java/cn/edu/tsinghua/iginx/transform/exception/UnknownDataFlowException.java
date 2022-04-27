package cn.edu.tsinghua.iginx.transform.exception;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;

public class UnknownDataFlowException extends TransformException {

    private static final long serialVersionUID = -7704462845267832343L;

    public UnknownDataFlowException(DataFlowType dataFlowType) {
        super("Unknown data flow type " + dataFlowType.toString());
    }
}
