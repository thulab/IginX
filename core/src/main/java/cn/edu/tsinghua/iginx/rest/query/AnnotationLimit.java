package cn.edu.tsinghua.iginx.rest.query;

public class AnnotationLimit
{
    private String tag = ".*";
    private String text = ".*";
    private String title = ".*";

    public void setTag(String tag)
    {
        this.tag = tag;
    }

    public void setText(String text)
    {
        this.text = text;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public String getTag()
    {
        return tag;
    }

    public String getText()
    {
        return text;
    }

    public String getTitle()
    {
        return title;
    }
}
