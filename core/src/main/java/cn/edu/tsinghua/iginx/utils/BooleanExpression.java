package cn.edu.tsinghua.iginx.utils;


import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class BooleanExpression
{
    private String boolExpression;
    private List<Element> postfixExpression;
    private TreeNode root;
    public BooleanExpression(String str)
    {
        boolExpression = str;
        postfixExpression = analysis(str);
        root = build(postfixExpression);
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
        String[] tmp = str.replace("("," ( ")
                .replace(")"," ) ")
                .replace(">=","__1")
                .replace("<=","__2")
                .replace("!=","__3")
                .replace("<>","__3")
                .replace(">"," > ")
                .replace("="," = ")
                .replace("<"," < ")
                .replace("__1"," >= ")
                .replace("__2"," <= ")
                .replace("__3"," != ")
                .split("\\s+");
        for (int i = 0; i < tmp.length; i++)
        {
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
        if (infixExpression.get(i).getType() == Type.OPERATOR
                && infixExpression.get(i).getOperator().getOperatorType() == OperatorType.NOT
                && i + 2 < n
                && infixExpression.get(i + 1).getType() == Type.VALUE
                && infixExpression.get(i + 2).getType() == Type.OPERATOR
                && getOperatorPriority(infixExpression.get(i).getOperator().getOperatorType()) == 2)
        {
            infixExpression.get(i + 2).getOperator().reverse();
        }
        return ret;
    }

    private List<Element> analysis(String str)
    {
        List<Element> infixExpression = getInfixExpression(str);
        List<Element> postfixExpression = new ArrayList<>();
        Stack<Element> operatorStack = new Stack<>();
        for (Element e: infixExpression)
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
                            postfixExpression.get(postfixExpression.size()).getOperator().reverse();
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
                if(stack.isEmpty() || stack.size()<2)
                {
                    return null;
                }
                ins.setLeft(stack.pop());
                ins.setRight(stack.pop());
                stack.push(ins);
            }
            else
            {
                stack.push(ins);
            }
        }
        if(stack.isEmpty() || stack.size()>1)
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
}

enum Type
{
    OPERATOR,
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
        if (operatorType == OperatorType.GT) operatorType = OperatorType.LT;
        if (operatorType == OperatorType.GTE) operatorType = OperatorType.LTE;
        if (operatorType == OperatorType.EQ) operatorType = OperatorType.NE;
        if (operatorType == OperatorType.NE) operatorType = OperatorType.EQ;
        if (operatorType == OperatorType.LTE) operatorType = OperatorType.GTE;
        if (operatorType == OperatorType.LT) operatorType = OperatorType.GT;
        if (operatorType == OperatorType.AND || operatorType == OperatorType.OR)
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