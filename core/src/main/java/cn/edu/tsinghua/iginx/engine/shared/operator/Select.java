package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.data.Source;

// TODO: 行选择方式 @WYL
public class Select extends AbstractUnaryOperator {

    public Select(Source source) {
        super(OperatorType.Select, source);
    }
}
