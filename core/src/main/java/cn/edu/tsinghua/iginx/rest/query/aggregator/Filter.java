package cn.edu.tsinghua.iginx.rest.query.aggregator;

public class Filter
{
    private String op;
    private double value;

    public Filter(String op, double value)
    {
        this.op = op;
        this.value = value;
    }

    public void setOp(String op)
    {
        this.op = op;
    }

    public void setValue(double value)
    {
        this.value = value;
    }

    public double getValue()
    {
        return value;
    }

    public String getOp()
    {
        return op;
    }
}
