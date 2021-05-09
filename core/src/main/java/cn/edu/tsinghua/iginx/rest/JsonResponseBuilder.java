package cn.edu.tsinghua.iginx.rest;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class JsonResponseBuilder
{
    private List<String> errorMessages = new ArrayList<>();
    private int status;

    public JsonResponseBuilder(Response.Status status) {
        this.status = status.getStatusCode();
    }

    public JsonResponseBuilder addErrors(List<String> errorMessages) {
        this.errorMessages.addAll(errorMessages);
        return this;
    }

    public JsonResponseBuilder addError(String errorMessage) {
        errorMessages.add(errorMessage);
        return this;
    }

    public Response build() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{\"errors\":[");
        for (String msg : errorMessages) {
            stringBuilder.append("\"");
            stringBuilder.append(msg);
            stringBuilder.append("\",");
        }
        if (!errorMessages.isEmpty()) {
            stringBuilder.deleteCharAt(stringBuilder.length()-1);
        }
        stringBuilder.append("]}");

        return Response
                .status(status)
                .header("Access-Control-Allow-Origin", "*")
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(stringBuilder.toString()).build();
    }


}
