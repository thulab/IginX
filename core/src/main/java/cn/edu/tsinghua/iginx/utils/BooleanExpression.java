package cn.edu.tsinghua.iginx.utils;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class BooleanExpression
{
    private String boolExpression;
    private List<Element> postfixExpression;
    private TreeNode root;
    private List<String> timeseries = new ArrayList<>();

    public BooleanExpression(String str)
    {
        boolExpression = str;
        postfixExpression = analysis(str);
        root = build(postfixExpression);
        dfs(root);
    }

    public boolean getBool(Map<String, Object> values)
    {
        return getAnswer(root, values);
    }


    private boolean getAnswer(TreeNode now, Map<String, Object> values)
    {
        if (now.getData().getType() == Type.OPERATOR && getOperatorPriority(now.getData().getOperator().getOperatorType()) == 0)
        {
            try
            {
                Object value = values.get(now.getLeft().getData().getTimeseries());
                String compareValue = now.getRight().getData().getValue();
                if (value instanceof Long)
                {
                    long longValue = Long.parseLong(compareValue);
                    if (isBool((Long)value, longValue, now.getData().getOperator()))
                        return true;
                    return false;
                }
                else if (value instanceof Double)
                {
                    double doubleValue = Double.parseDouble(compareValue);
                    if (isBool((Double)value, doubleValue,  now.getData().getOperator()))
                        return true;
                    return false;
                }
                else if (value instanceof String)
                {
                    if (isBool((String)value, compareValue,  now.getData().getOperator()))
                        return true;
                    return false;
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                return false;
            }
        }
        else
        {
            Boolean leftAns = getAnswer(now.getLeft(), values);
            Boolean rightAns = getAnswer(now.getLeft(), values);
            if (now.getData().getOperator().getOperatorType() == OperatorType.AND)
                return leftAns && rightAns;
            else
                return leftAns || rightAns;
        }
    }

    private Boolean isBool(long lef, long rig, Operator op)
    {
        switch (op.getOperatorType())
        {
            case GT:
                return lef > rig;
            case GTE:
                return lef >= rig;
            case EQ:
                return lef == rig;
            case NE:
                return lef != rig;
            case LTE:
                return lef <= rig;
            case LT:
                return lef < rig;
            default:
                return false;
        }
    }


    private Boolean isBool(double lef, double rig, Operator op)
    {
        switch (op.getOperatorType())
        {
            case GT:
                return lef > rig;
            case GTE:
                return lef >= rig;
            case EQ:
                return lef == rig;
            case NE:
                return lef != rig;
            case LTE:
                return lef <= rig;
            case LT:
                return lef < rig;
            default:
                return false;
        }
    }



    private Boolean isBool(String lef, String rig, Operator op)
    {
        switch (op.getOperatorType())
        {
            case GT:
                return lef.compareTo(rig) > 0;
            case GTE:
                return lef.compareTo(rig) >= 0;
            case EQ:
                return lef.compareTo(rig) == 0;
            case NE:
                return lef.compareTo(rig) != 0;
            case LTE:
                return lef.compareTo(rig) <= 0;
            case LT:
                return lef.compareTo(rig) < 0;
            default:
                return false;
        }
    }

    private void dfs(TreeNode now)
    {
        if (now.getData().getType() == Type.OPERATOR && now.getData().getOperator().reverse)
        {
            now.getData().getOperator().reverse = false;
            if (now.getData().getOperator().operatorType == OperatorType.AND ||
                    now.getData().getOperator().operatorType == OperatorType.OR)
            {
                now.getLeft().getData().getOperator().reverse();
                now.getRight().getData().getOperator().reverse();
                if (now.getData().getOperator().operatorType == OperatorType.AND)
                    now.getData().setOperator(new Operator(OperatorType.OR));
                else
                    now.getData().setOperator(new Operator(OperatorType.AND));
            }
            else
                now.getData().getOperator().reverse();
        }
        if (now.getData().getType() == Type.TIMESERIES)
            timeseries.add(now.getData().getTimeseries());
        if (now.getLeft() != null)
            dfs(now.getLeft());
        if (now.getRight() != null)
            dfs(now.getRight());
    }


    public int getOperatorPriority(OperatorType tp)
    {
        switch (tp)
        {

            case GT:
            case GTE:
            case EQ:
            case NE:
            case LTE:
            case LT:
                return 0;
            case NOT:
                return 1;
            case AND:
                return 2;
            case OR:
                return 3;
            case LEFTBRACKET:
                return 4;
            case RIGHTBRACKET:
                return 5;
            default:
                return 6;
        }
    }

    private List<Element> getInfixExpression(String str)
    {
        List<Element> infixExpression = new ArrayList<>();
        String[] tmp = str.replace("(", " ( ")
                .replace(")", " ) ")
                .replace(">=", "__1")
                .replace("<=", "__2")
                .replace("!=", "__3")
                .replace("<>", "__3")
                .replace(">", " > ")
                .replace("=", " = ")
                .replace("<", " < ")
                .replace("&&", " && ")
                .replace("||", " || ")
                .replace("!", " ! ")
                .replace("__1", " >= ")
                .replace("__2", " <= ")
                .replace("__3", " != ")
                .split("\\s+");
        for (int i = 0; i < tmp.length; i++)
        {
            if (tmp[i].length() == 0) continue;
            Element ins = new Element();
            switch (tmp[i])
            {
                case "(":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.LEFTBRACKET));
                    break;
                case ")":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.RIGHTBRACKET));
                    break;
                case ">":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.GT));
                    break;
                case ">=":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.GTE));
                    break;
                case "=":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.EQ));
                    break;
                case "!=":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.NE));
                    break;
                case "<=":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.LTE));
                    break;
                case "<":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.LT));
                    break;
                case "!":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.NOT));
                    break;
                case "&&":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.AND));
                    break;
                case "||":
                    ins.setType(Type.OPERATOR);
                    ins.setOperator(new Operator(OperatorType.OR));
                    break;
                default:
                    String op = tmp[i].toLowerCase();
                    switch (op)
                    {
                        case "not":
                            ins.setType(Type.OPERATOR);
                            ins.setOperator(new Operator(OperatorType.NOT));
                            break;
                        case "and":
                            ins.setType(Type.OPERATOR);
                            ins.setOperator(new Operator(OperatorType.AND));
                            break;
                        case "or":
                            ins.setType(Type.OPERATOR);
                            ins.setOperator(new Operator(OperatorType.OR));
                            break;
                        default:
                            ins.setType(Type.VALUE);
                            ins.setValue(tmp[i]);

                    }
                    break;
            }
            infixExpression.add(ins);
        }
        List<Element> ret = new ArrayList<>();
        int n = infixExpression.size();
        for (int i = 0; i < n; i++)
        {
            if (infixExpression.get(i).getType() == Type.OPERATOR
                    && infixExpression.get(i).getOperator().getOperatorType() == OperatorType.NOT
                    && i + 2 < n
                    && infixExpression.get(i + 1).getType() == Type.VALUE
                    && infixExpression.get(i + 2).getType() == Type.OPERATOR
                    && getOperatorPriority(infixExpression.get(i + 2).getOperator().getOperatorType()) == 0)
            {
                infixExpression.get(i + 2).getOperator().reverse();
            }
            else
            {
                ret.add(infixExpression.get(i));
            }
        }
        Element end = new Element();
        end.setType(Type.OPERATOR);
        end.setOperator(new Operator(OperatorType.END));
        ret.add(end);
        return ret;
    }

    public String asString(List<Element> elements)
    {
        StringBuilder stringBuilder = new StringBuilder("");
        for (Element element : elements)
        {
            if (element.getType() == Type.VALUE)
                stringBuilder.append(" " + element.getValue() + " ");
            else switch (element.getOperator().operatorType)
            {
                case NOT:
                    stringBuilder.append(" ! ");
                    break;
                case AND:
                    stringBuilder.append(" && ");
                    break;
                case OR:
                    stringBuilder.append(" || ");
                    break;
                case GT:
                    stringBuilder.append(" > ");
                    break;
                case GTE:
                    stringBuilder.append(" >= ");
                    break;
                case EQ:
                    stringBuilder.append(" == ");
                    break;
                case NE:
                    stringBuilder.append(" != ");
                    break;
                case LTE:
                    stringBuilder.append(" <= ");
                    break;
                case LT:
                    stringBuilder.append(" < ");
                    break;
                case LEFTBRACKET:
                    stringBuilder.append("(");
                    break;
                case RIGHTBRACKET:
                    stringBuilder.append(")");
                    break;
                default:
                    break;
            }
        }
        return stringBuilder.toString();
    }

    private List<Element> analysis(String str)
    {
        List<Element> infixExpression = getInfixExpression(str);
        List<Element> postfixExpression = new ArrayList<>();
        Stack<Element> operatorStack = new Stack<>();
        for (Element e : infixExpression)
        {
            if (e.getType() == Type.VALUE)
                postfixExpression.add(e);
            else
            {
                switch (e.getOperator().getOperatorType())
                {
                    case LEFTBRACKET:
                    case NOT:
                        operatorStack.push(e);
                        break;
                    case RIGHTBRACKET:
                        while (!operatorStack.empty() &&
                                operatorStack.peek().getOperator().getOperatorType() != OperatorType.LEFTBRACKET)
                        {
                            postfixExpression.add(operatorStack.pop());
                        }
                        operatorStack.pop();
                        if (!operatorStack.empty() &&
                                operatorStack.peek().getOperator().getOperatorType() == OperatorType.NOT)
                        {
                            postfixExpression.get(postfixExpression.size() - 1).getOperator().reverse();
                            operatorStack.pop();
                        }
                        break;
                    case GT:
                    case GTE:
                    case EQ:
                    case NE:
                    case LTE:
                    case LT:
                    case AND:
                    case OR:
                    case END:
                        int now = getOperatorPriority(e.getOperator().getOperatorType());
                        while (!operatorStack.empty() &&
                                getOperatorPriority(operatorStack.peek().getOperator().getOperatorType()) <= now)
                        {
                            postfixExpression.add(operatorStack.pop());
                        }
                        operatorStack.push(e);
                        break;
                }
            }
        }
        return postfixExpression;
    }

    private TreeNode build(List<Element> postfixExpression)
    {
        if (postfixExpression.size() == 0)
            return null;
        Stack<TreeNode> stack = new Stack<>();
        for (int i = 0; i < postfixExpression.size(); i++)
        {
            TreeNode ins = new TreeNode();
            ins.setData(postfixExpression.get(i));
            if (postfixExpression.get(i).getType() == Type.OPERATOR)
            {
                if (stack.isEmpty() || stack.size() < 2)
                {
                    return null;
                }
                ins.setRight(stack.pop());
                ins.setLeft(stack.pop());
                if (ins.getLeft().getData().getType() == Type.VALUE)
                {
                    ins.getLeft().getData().setType(Type.TIMESERIES);
                    String tmp = ins.getLeft().getData().getValue();
                    ins.getLeft().getData().setTimeseries(tmp);
                }
                stack.push(ins);
            }
            else
            {
                stack.push(ins);
            }
        }
        if (stack.isEmpty() || stack.size() > 1)
        {
            return null;
        }
        return stack.pop();
    }

    public String getBoolExpression()
    {
        return boolExpression;
    }

    public List<Element> getPostfixExpression()
    {
        return postfixExpression;
    }

    public TreeNode getRoot()
    {
        return root;
    }

    public void setBoolExpression(String boolExpression)
    {
        this.boolExpression = boolExpression;
    }

    public void setPostfixExpression(List<Element> postfixExpression)
    {
        this.postfixExpression = postfixExpression;
    }

    public void setRoot(TreeNode root)
    {
        this.root = root;
    }

    public List<String> getTimeseries()
    {
        return timeseries;
    }

    public void setTimeseries(List<String> timeseries)
    {
        this.timeseries = timeseries;
    }
}

class TreeNode
{
    private Element data;
    private TreeNode left;
    private TreeNode right;

    public Element getData()
    {
        return data;
    }

    public TreeNode getLeft()
    {
        return left;
    }

    public TreeNode getRight()
    {
        return right;
    }

    public void setData(Element data)
    {
        this.data = data;
    }

    public void setLeft(TreeNode left)
    {
        this.left = left;
    }

    public void setRight(TreeNode right)
    {
        this.right = right;
    }
}

class Element
{
    Type type;
    Operator operator;
    String timeseries;
    String value;

    public Type getType()
    {
        return type;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public String getValue()
    {
        return value;
    }

    public void setType(Type type)
    {
        this.type = type;
    }

    public void setOperator(Operator operator)
    {
        this.operator = operator;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public String getTimeseries()
    {
        return timeseries;
    }

    public void setTimeseries(String timeseries)
    {
        this.timeseries = timeseries;
    }
}

enum Type
{
    OPERATOR,
    TIMESERIES,
    VALUE;
}

class Operator
{
    OperatorType operatorType;
    boolean reverse = false;

    Operator(OperatorType tp)
    {
        operatorType = tp;
    }

    public OperatorType getOperatorType()
    {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType)
    {
        this.operatorType = operatorType;
    }

    void reverse()
    {
        if (operatorType == OperatorType.GT) operatorType = OperatorType.LTE;
        else if (operatorType == OperatorType.GTE) operatorType = OperatorType.LT;
        else if (operatorType == OperatorType.EQ) operatorType = OperatorType.NE;
        else if (operatorType == OperatorType.NE) operatorType = OperatorType.EQ;
        else if (operatorType == OperatorType.LTE) operatorType = OperatorType.GT;
        else if (operatorType == OperatorType.LT) operatorType = OperatorType.GTE;
        else if (operatorType == OperatorType.AND || operatorType == OperatorType.OR)
            reverse = true;
    }
}

enum OperatorType
{
    LEFTBRACKET,
    RIGHTBRACKET,
    GT,
    GTE,
    EQ,
    NE,
    LTE,
    LT,
    NOT,
    AND,
    OR,
    END;
}