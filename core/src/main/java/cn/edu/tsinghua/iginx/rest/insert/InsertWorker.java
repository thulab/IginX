package cn.edu.tsinghua.iginx.rest.insert;

import cn.edu.tsinghua.iginx.rest.JsonResponseBuilder;
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

public class InsertWorker extends Thread
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertWorker.class);
    private HttpHeaders httpheaders;
    private InputStream stream;
    private AsyncResponse asyncResponse;

    public InsertWorker(final AsyncResponse asyncResponse, HttpHeaders httpheaders,
                        InputStream stream)
    {
        this.asyncResponse = asyncResponse;
        this.httpheaders = httpheaders;
        this.stream = stream;
    }


    @Override
    public void run()
    {
        Response response;
        try
        {
            if (httpheaders != null)
            {
                List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
                if (requestHeader != null && requestHeader.contains("gzip"))
                {
                    stream = new GZIPInputStream(stream);
                }
            }
            DataPointsParser parser = new DataPointsParser(new InputStreamReader(stream, StandardCharsets.UTF_8));
            parser.parse();
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.OK);
            response = builder.build();
        }
        catch (Exception e)
        {
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            response = builder.addError(e.getMessage()).build();
        }
        asyncResponse.resume(response);
    }
}
