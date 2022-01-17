package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

import java.util.List;

public class RemoveNotOptimizer implements Optimizer {

    private final static RemoveNotOptimizer instance = new RemoveNotOptimizer();

    private RemoveNotOptimizer() {
    }

    public static RemoveNotOptimizer getInstance() {
        return instance;
    }

    @Override
    public Operator optimize(Operator root) {
        removeNot(root);
        return root;
    }

    private void removeNot(Operator operator) {
        // remove not filter.
        if (operator.getType() == OperatorType.Select) {
            Select select = (Select) operator;
            Filter filter = ExprUtils.removeNot(select.getFilter());
            select.setFilter(filter);
        }

        // dfs to find select operator.
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOp = (UnaryOperator) operator;
            Source source = unaryOp.getSource();
            if (source.getType() != SourceType.Fragment) {
                removeNot(((OperatorSource) source).getOperator());
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            removeNot(((OperatorSource) binaryOperator.getSourceA()).getOperator());
            removeNot(((OperatorSource) binaryOperator.getSourceB()).getOperator());
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            for (Source source: sources) {
                removeNot(((OperatorSource) source).getOperator());
            }
        }
    }
}
