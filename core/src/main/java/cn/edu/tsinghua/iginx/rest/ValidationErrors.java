package cn.edu.tsinghua.iginx.rest;

import java.util.ArrayList;
import java.util.List;

public class ValidationErrors
{
    private List<String> errors = new ArrayList<String>();

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void addErrorMessage(String message) {
        errors.add(message);
    }

    public void add(ValidationErrors errors) {
        this.errors.addAll(errors.getErrors());
    }

    public boolean hasErrors() {
        return errors.size() > 0;
    }

    public List<String> getErrors() {
        return errors;
    }

    public String getFirstError() {
        return errors.get(0);
    }

    public int size() {
        return errors.size();
    }
}
