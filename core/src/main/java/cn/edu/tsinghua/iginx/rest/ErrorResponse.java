package cn.edu.tsinghua.iginx.rest;

import java.util.Collections;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ErrorResponse
{
    private List<String> m_errors;

    @JsonCreator
    public ErrorResponse(@JsonProperty("errors") List<String> errors) {
        m_errors = errors;
    }

    public ErrorResponse(String error) {
        m_errors = Collections.singletonList(error);
    }

    @JsonProperty
    public List<String> getErrors() {
        return (m_errors);
    }
}
