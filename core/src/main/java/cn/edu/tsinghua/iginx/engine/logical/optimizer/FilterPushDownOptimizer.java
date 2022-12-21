package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FilterPushDownOptimizer implements Optimizer {

    private final static Logger logger = LoggerFactory.getLogger(FilterPushDownOptimizer.class);

    private static FilterPushDownOptimizer instance;

    public static FilterPushDownOptimizer getInstance() {
        if (instance == null) {
            synchronized (FilterFragmentOptimizer.class) {
                if (instance == null) {
                    instance = new FilterPushDownOptimizer();
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
            pushDown(selectOperator);
        }
        return root;
    }

    private void pushDown(Select selectOperator) {
        List<Pair<Project, Operator>> projectAndFatherOperatorList = new ArrayList<>();
        Stack<Operator> stack = new Stack<>();
        findProjectUpperFragment(projectAndFatherOperatorList, stack, selectOperator);

        if (projectAndFatherOperatorList.size() == 0) {
            logger.error("There is no project operator just upper fragment in select tree.");
            return;
        }

        Filter filter = selectOperator.getFilter().copy();
        TagFilter tagFilter = selectOperator.getTagFilter().copy();
        Map<String, Filter> cache = new HashMap<>();
        for (Pair<Project, Operator> pair : projectAndFatherOperatorList) {
            Project project = pair.getK();
            FragmentMeta fragmentMeta = ((FragmentSource) project.getSource()).getFragment();

            // the same meta just call once.
            Filter subFilter;
            if (cache.containsKey(fragmentMeta.getMasterStorageUnitId())) {
                subFilter = cache.get(fragmentMeta.getMasterStorageUnitId()).copy();
            } else {
                subFilter = ExprUtils.getSubFilterFromFragment(filter, fragmentMeta.getTsInterval());
                cache.put(fragmentMeta.getMasterStorageUnitId(), subFilter);
            }
            if (subFilter.getType() == FilterType.Bool && ((BoolFilter) subFilter).isTrue()) {
                // need to scan whole scope.
                return;
            }
            Select subSelect = new Select(new OperatorSource(project), filter, tagFilter);

            Operator fatherOperator = pair.getV();
            if (fatherOperator != null) {
                if (OperatorType.isUnaryOperator(fatherOperator.getType())) {
                    UnaryOperator unaryOp = (UnaryOperator) fatherOperator;
                    unaryOp.setSource(new OperatorSource(subSelect));
                } else if (OperatorType.isBinaryOperator(fatherOperator.getType())) {
                    BinaryOperator binaryOperator = (BinaryOperator) fatherOperator;
                    Operator operatorA = ((OperatorSource) binaryOperator.getSourceA()).getOperator();
                    Operator operatorB = ((OperatorSource) binaryOperator.getSourceB()).getOperator();

                    if (operatorA.equals(project)) {
                        binaryOperator.setSourceA(new OperatorSource(subSelect));
                    } else if (operatorB.equals(project)) {
                        binaryOperator.setSourceB(new OperatorSource(subSelect));
                    }
                } else {
                    MultipleOperator multipleOperator = (MultipleOperator) fatherOperator;
                    List<Source> sources = multipleOperator.getSources();

                    int index = -1;
                    for (int i = 0; i < sources.size(); i++) {
                        Operator curOperator = ((OperatorSource) sources.get(i)).getOperator();
                        if (curOperator.equals(project)) {
                            index = i;
                        }
                    }
                    if (index != -1) {
                        sources.set(index, new OperatorSource(subSelect));
                    }
                    multipleOperator.setSources(sources);
                }
            }
        }
    }

    private void findProjectUpperFragment(List<Pair<Project, Operator>> projectAndFatherOperatorList, Stack<Operator> stack, Operator operator) {
        // dfs to find project operator just upper fragment and his father operator.
        stack.push(operator);
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOp = (UnaryOperator) operator;
            Source source = unaryOp.getSource();
            if (source.getType() == SourceType.Fragment) {
                Project project = (Project) stack.pop();
                Operator father = stack.isEmpty() ? null : stack.peek();
                projectAndFatherOperatorList.add(new Pair<>(project, father));
                return;
            } else {
                findProjectUpperFragment(projectAndFatherOperatorList, stack, ((OperatorSource) source).getOperator());
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            findProjectUpperFragment(projectAndFatherOperatorList, stack, ((OperatorSource) binaryOperator.getSourceA()).getOperator());
            findProjectUpperFragment(projectAndFatherOperatorList, stack, ((OperatorSource) binaryOperator.getSourceB()).getOperator());
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            for (Source source : sources) {
                findProjectUpperFragment(projectAndFatherOperatorList, stack, ((OperatorSource) source).getOperator());
            }
        }
        stack.pop();
    }
}
