package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.inject.Inject;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/v1")
public class MetricsResource {

    public static Map<String, Map<String, Integer>> schemamapping = new ConcurrentHashMap<>();
    private static final String QUERY_URL = "/datapoints/query";
    private static final String NO_CACHE = "no-cache";
    private static final ExecutorService threadPool = Executors.newCachedThreadPool();
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);

    private final Gson gson;

    @Inject
    public MetricsResource()
    {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.disableHtmlEscaping().create();
    }

    static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder)
    {
        responseBuilder.header("Access-Control-Allow-Origin", "*");
        responseBuilder.header("Pragma", NO_CACHE);
        responseBuilder.header("Cache-Control", NO_CACHE);
        responseBuilder.header("Expires", 0);
        return (responseBuilder);
    }


    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/datapoints")
    public void add(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse)
    {
        threadPool.execute(new IngestionWorker(asyncResponse, httpheaders, stream));
    }


    private static String inputStreamToString(InputStream inputStream) throws UnsupportedEncodingException
    {
        StringBuffer buffer = new StringBuffer();
        InputStreamReader inputStreamReader  = new InputStreamReader(inputStream, "utf-8");;
        try {
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                buffer.append(str);
            }
            // 释放资源
            try
            {
                bufferedReader.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            inputStreamReader.close();
            inputStream.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path(QUERY_URL)
    public Response getQuery(@Context HttpHeaders httpheaders, final InputStream stream)
    {
        try
        {
            return getQuery(inputStreamToString(stream));
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public Response getQuery(String jsonStr)
    {
        try
        {
            if (jsonStr == null)
            {
                throw new Exception("query json must not be null or empty");
            }
            long start = System.currentTimeMillis();
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute();
            String entity = parser.parseResultToJson(result);
            return Response.status(Status.OK).header("Access-Control-Allow-Origin", "*").header("Pragma", NO_CACHE)
                .header("Cache-Control", NO_CACHE).header("Expires", 0)
                .entity(entity).build();

        }
        catch (Exception e)
        {
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            List<String> ret = new ArrayList<>();
            ret.add(e.getMessage());
            return builder.addErrors(ret).build();
        }
    }
}