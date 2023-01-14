package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;

public class OperatorUtils {

    public static Operator unionOperators(List<Operator> operators) {
        if (operators == null || operators.isEmpty())
            return null;
        if (operators.size() == 1)
            return operators.get(0);
        Operator union = operators.get(0);
        for (int i = 1; i < operators.size(); i++) {
            union = new Union(new OperatorSource(union), new OperatorSource(operators.get(i)));
        }
        return union;
    }

    public static Operator joinOperatorsByTime(List<Operator> operators) {
        return joinOperators(operators, KEY);
    }

    public static Operator joinOperators(List<Operator> operators, String joinBy) {
        if (operators == null || operators.isEmpty())
            return null;
        if (operators.size() == 1)
            return operators.get(0);
        Operator join = operators.get(0);
        for (int i = 1; i < operators.size(); i++) {
            join = new Join(new OperatorSource(join), new OperatorSource(operators.get(i)), joinBy);
        }
        return join;
    }

    public static List<String> findPathList(Operator operator) {
        List<Project> projectList = new ArrayList<>();
        findProjectOperators(projectList, operator);

        if (projectList.isEmpty()) {
            return new ArrayList<>();
        } else {
            return projectList.get(0).getPatterns();
        }
    }

    public static void findProjectOperators(List<Project> projectOperatorList, Operator operator) {
        if (operator.getType() == OperatorType.Project) {
            projectOperatorList.add((Project) operator);
            return;
        }

        // dfs to find project operator.
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOp = (UnaryOperator) operator;
            Source source = unaryOp.getSource();
            if (source.getType() != SourceType.Fragment) {
                findProjectOperators(projectOperatorList, ((OperatorSource) source).getOperator());
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            findProjectOperators(projectOperatorList, ((OperatorSource) binaryOperator.getSourceA()).getOperator());
            findProjectOperators(projectOperatorList, ((OperatorSource) binaryOperator.getSourceB()).getOperator());
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            for (Source source : sources) {
                findProjectOperators(projectOperatorList, ((OperatorSource) source).getOperator());
            }
        }
    }

    public static void findSelectOperators(List<Select> selectOperatorList, Operator operator) {
        if (operator.getType() == OperatorType.Select) {
            selectOperatorList.add((Select) operator);
            return;
        }

        // dfs to find select operator.
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOp = (UnaryOperator) operator;
            Source source = unaryOp.getSource();
            if (source.getType() != SourceType.Fragment) {
                findSelectOperators(selectOperatorList, ((OperatorSource) source).getOperator());
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            findSelectOperators(selectOperatorList, ((OperatorSource) binaryOperator.getSourceA()).getOperator());
            findSelectOperators(selectOperatorList, ((OperatorSource) binaryOperator.getSourceB()).getOperator());
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            for (Source source : sources) {
                findSelectOperators(selectOperatorList, ((OperatorSource) source).getOperator());
            }
        }
    }
}
