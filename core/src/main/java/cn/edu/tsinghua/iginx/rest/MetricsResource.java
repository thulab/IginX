package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.*;
import javax.inject.Inject;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.rest.insert.InsertWorker;
import cn.edu.tsinghua.iginx.rest.query.Query;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.rest.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/v1")
public class MetricsResource {

    private static final String QUERY_URL = "/datapoints/query";
    private static final String DELETE_URL = "/datapoints/delete";
    private static final String NO_CACHE = "no-cache";
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(100);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);


    @Inject
    public MetricsResource()
    {

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
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream));
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
    public Response postQuery(@Context HttpHeaders httpheaders, final InputStream stream)
    {
        try
        {
            return postQuery(inputStreamToString(stream));
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public Response postQuery(String jsonStr)
    {
        try
        {
            if (jsonStr == null)
            {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(false);
            String entity = parser.parseResultToJson(result, false);
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

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path(DELETE_URL)
    public Response postDelete(@Context HttpHeaders httpheaders, final InputStream stream)
    {
        try
        {
            return postDelete(inputStreamToString(stream));
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public Response postDelete(String jsonStr)
    {
        try
        {
            if (jsonStr == null)
            {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(true);
            String entity = parser.parseResultToJson(result, true);
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


    @DELETE
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/metric/{metricName}")
    public Response metricDelete(@PathParam("metricName") String metricName)
    {
        deleteMetric(metricName);
        return setHeaders(Response.status(Response.Status.OK)).build();
    }

    void deleteMetric(String metricName)
    {
        RestSession restSession = new RestSession();
        try
        {
            restSession.openSession();
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        List<String> ins = new ArrayList<>();
        for (int i = 1; i < ConfigDescriptor.getInstance().getConfig().getMaxTimeseriesLength(); i++)
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 0; j < i; j++)
                stringBuilder.append("*.");
            stringBuilder.append(metricName);
            ins.add(stringBuilder.toString());
        }
        try
        {
            restSession.deleteColumns(ins);
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
        finally
        {
            restSession.closeSession();
        }
    }
}