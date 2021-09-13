package cn.edu.tsinghua.iginx.rest.query;

public class AnnotationLimit {
    private String tag = ".*";
    private String text = ".*";
    private String title = ".*";

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
