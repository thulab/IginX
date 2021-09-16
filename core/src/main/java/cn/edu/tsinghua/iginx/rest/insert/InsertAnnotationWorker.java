package cn.edu.tsinghua.iginx.rest.insert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class InsertAnnotationWorker extends Thread {
    private static final String NO_CACHE = "no-cache";
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertAnnotationWorker.class);
    private HttpHeaders httpheaders;
    private InputStream stream;
    private AsyncResponse asyncResponse;

    public InsertAnnotationWorker(final AsyncResponse asyncResponse, HttpHeaders httpheaders,
                                  InputStream stream) {
        this.asyncResponse = asyncResponse;
        this.httpheaders = httpheaders;
        this.stream = stream;
    }

    static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder) {
        responseBuilder.header("Access-Control-Allow-Origin", "*");
        responseBuilder.header("Pragma", NO_CACHE);
        responseBuilder.header("Cache-Control", NO_CACHE);
        responseBuilder.header("Expires", 0);
        return (responseBuilder);
    }

    @Override
    public void run() {
        Response response;
        try {
            if (httpheaders != null) {
                List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
                if (requestHeader != null && requestHeader.contains("gzip")) {
                    stream = new GZIPInputStream(stream);
                }
            }
            DataPointsParser parser = new DataPointsParser(new InputStreamReader(stream, StandardCharsets.UTF_8));
            parser.parseAnnotation();
            response = Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            response = setHeaders(Response.status(Response.Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
        asyncResponse.resume(response);
    }
}
