package cn.edu.tsinghua.iginx.utils;


import java.util.ArrayList;
import java.util.List;

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

    private List<Element> analysis(String str)
    {
        List<Element> ret = new ArrayList<>();
        return ret;
    }

    private TreeNode build(List<Element> postfixExpression)
    {
        return null;
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
    String timeseries;
    CompareValue value;

    public Type getType()
    {
        return type;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public String getTimeseries()
    {
        return timeseries;
    }

    public CompareValue getValue()
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

    public void setTimeseries(String timeseries)
    {
        this.timeseries = timeseries;
    }

    public void setValue(CompareValue value)
    {
        this.value = value;
    }
}

enum Type
{
    OPERATOR,
    TIMESERIES,
    CompareValue;
}

class Operator
{
    OperatorType operatorType;
    Boolean isNot = false;
}

enum OperatorType
{
    LEFTBRACKET,
    RIGHTBRACKET,
    NOT,
    AND,
    OR,
    GT,
    GTE,
    EQ,
    NE,
    LTE,
    LT;
    public int getOperatorPriority(OperatorType tp)
    {
        switch (tp)
        {
            case LEFTBRACKET:
                return 0;
            case RIGHTBRACKET:
                return 1;
            case NOT:
                return 2;
            case AND:
                return 3;
            case OR:
                return 4;
            case GT:
            case GTE:
            case EQ:
            case NE:
            case LTE:
            case LT:
            default:
                return 5;
        }
    }
}

class CompareValue
{
    DataType dataType;
    Long longValue;
    Double doubleValue;
    String stringValue;

    public DataType getDataType()
    {
        return dataType;
    }

    public Long getLongValue()
    {
        return longValue;
    }

    public Double getDoubleValue()
    {
        return doubleValue;
    }

    public String getStringValue()
    {
        return stringValue;
    }

    public void setDataType(DataType dataType)
    {
        this.dataType = dataType;
    }

    public void setLongValue(Long longValue)
    {
        this.longValue = longValue;
    }

    public void setDoubleValue(Double doubleValue)
    {
        this.doubleValue = doubleValue;
    }

    public void setStringValue(String stringValue)
    {
        this.stringValue = stringValue;
    }
}


enum DataType
{
    LONG,
    DOUBLE,
    STRING;
}