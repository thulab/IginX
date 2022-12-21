package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RemoveNotOptimizer implements Optimizer {

    private final static Logger logger = LoggerFactory.getLogger(RemoveNotOptimizer.class);

    private static RemoveNotOptimizer instance;

    private RemoveNotOptimizer() {
    }

    public static RemoveNotOptimizer getInstance() {
        if (instance == null) {
            synchronized (FilterFragmentOptimizer.class) {
                if (instance == null) {
                    instance = new RemoveNotOptimizer();
                }
            }
        }
        return instance;
    }

    @Override
    public Operator optimize(Operator root) {
        // only optimize query
        if (root.getType() == OperatorType.CombineNonQuery || root.getType() == OperatorType.ShowTimeSeries) {
            return root;
        }

        List<Select> selectOperatorList = new ArrayList<>();
        OperatorUtils.findSelectOperators(selectOperatorList, root);

        if (selectOperatorList.isEmpty()) {
            logger.info("There is no filter in logical tree.");
            return root;
        }

        for (Select selectOperator : selectOperatorList) {
            removeNot(selectOperator);
        }
        return root;
    }

    private void removeNot(Select selectOperator) {
        // remove not filter.
        Filter filter = ExprUtils.removeNot(selectOperator.getFilter());
        selectOperator.setFilter(filter);
    }
}
