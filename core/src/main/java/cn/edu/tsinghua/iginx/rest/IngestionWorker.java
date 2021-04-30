package cn.edu.tsinghua.iginx.rest;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
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

public class IngestionWorker extends Thread
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionWorker.class);
    private static final String NO_CACHE = "no-cache";
    private HttpHeaders httpheaders;
    private InputStream stream;
    private Gson gson;
    private AsyncResponse asyncResponse;

    public IngestionWorker(final AsyncResponse asyncResponse, HttpHeaders httpheaders,
                           InputStream stream, Gson gson)
    {
        this.asyncResponse = asyncResponse;
        this.httpheaders = httpheaders;
        this.stream = stream;
        this.gson = gson;
    }


    @Override
    public void run()
    {
        Response response;
        try {
            if (httpheaders != null)
            {
                List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
                if (requestHeader != null && requestHeader.contains("gzip"))
                {
                    stream = new GZIPInputStream(stream);
                }
            }
            DataPointsParser parser = new DataPointsParser(new InputStreamReader(stream, StandardCharsets.UTF_8), gson);
            ValidationErrors validationErrors = parser.parse();
            if (!validationErrors.hasErrors())
            {
                response = setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
            }
            else
            {
                JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
                for (String errorMessage : validationErrors.getErrors())
                {
                    builder.addError(errorMessage);
                }
                response = builder.build();
            }
        }
        catch (JsonIOException | MalformedJsonException | JsonSyntaxException e)
        {
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            response = builder.addError(e.getMessage()).build();
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to add metric.", e);
            response = setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse(e.getMessage()))).build();
        }
        catch (OutOfMemoryError e)
        {
            LOGGER.error("Out of memory error.", e);
            response = setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse(e.getMessage()))).build();
        }
        asyncResponse.resume(response);
    }

    private Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder)
    {
        responseBuilder.header("Access-Control-Allow-Origin", "*");
        responseBuilder.header("Pragma", NO_CACHE);
        responseBuilder.header("Cache-Control", NO_CACHE);
        responseBuilder.header("Expires", 0);
        return (responseBuilder);
    }
}
