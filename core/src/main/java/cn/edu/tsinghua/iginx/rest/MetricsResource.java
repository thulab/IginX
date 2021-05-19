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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.insert.InsertWorker;
import cn.edu.tsinghua.iginx.rest.query.Query;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.rest.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class MetricsResource {

    private static final String INSERT_URL = "api/v1/datapoints";
    private static final String QUERY_URL = "api/v1/datapoints/query";
    private static final String DELETE_URL = "api/v1/datapoints/delete";
    private static final String DELETE_METRIC_URL = "api/v1/metric/{metricName}";
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
    @Path("{string : .+}")
    public Response errorPath(@PathParam("string") String str)
    {
        JsonResponseBuilder builder = new JsonResponseBuilder(Status.NOT_FOUND);
        List<String> ret = new ArrayList<>();
        ret.add("Wrong Path");
        return builder.addErrors(ret).build();
    }

    @POST
    @Path(INSERT_URL)
    public void add(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse)
    {
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream));
    }




    private static String inputStreamToString(InputStream inputStream) throws Exception
    {
        StringBuffer buffer = new StringBuffer();
        InputStreamReader inputStreamReader  = new InputStreamReader(inputStream, "utf-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String str;
        while ((str = bufferedReader.readLine()) != null)
        {
            buffer.append(str);
        }
        bufferedReader.close();
        inputStreamReader.close();
        inputStream.close();
        return buffer.toString();
    }

    @POST
    @Path(QUERY_URL)
    public Response postQuery(final InputStream stream)
    {
        try
        {
            String str = inputStreamToString(stream);
            return postQuery(str);
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during execution : {}", e.getMessage());
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            List<String> ret = new ArrayList<>();
            ret.add(e.getMessage());
            return builder.addErrors(ret).build();
        }
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
            return setHeaders(Response.status(Status.OK).entity(entity)).build();

        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during execution : {}", e.getMessage());
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            List<String> ret = new ArrayList<>();
            ret.add(e.getMessage());
            return builder.addErrors(ret).build();
        }
    }

    @POST
    @Path(DELETE_URL)
    public Response postDelete(final InputStream stream)
    {
        try
        {
            String str = inputStreamToString(stream);
            return postDelete(str);
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during execution : {}", e.getMessage());
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            List<String> ret = new ArrayList<>();
            ret.add(e.getMessage());
            return builder.addErrors(ret).build();
        }
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
            return setHeaders(Response.status(Status.OK).entity(entity)).build();
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during execution : {}", e.getMessage());
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            List<String> ret = new ArrayList<>();
            ret.add(e.getMessage());
            return builder.addErrors(ret).build();
        }
    }


    @DELETE
    @Path(DELETE_METRIC_URL)
    public Response metricDelete(@PathParam("metricName") String metricName)
    {
        try
        {
            deleteMetric(metricName);
            return setHeaders(Response.status(Response.Status.OK)).build();
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during execution : {}", e.getMessage());
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
            List<String> ret = new ArrayList<>();
            ret.add(e.getMessage());
            return builder.addErrors(ret).build();
        }
    }

    void deleteMetric(String metricName) throws Exception
    {
        RestSession restSession = new RestSession();
        restSession.openSession();
        List<String> ins = new ArrayList<>();
        for (int i = 1; i < ConfigDescriptor.getInstance().getConfig().getMaxTimeseriesLength(); i++)
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 0; j < i; j++)
                stringBuilder.append("*.");
            stringBuilder.append(metricName);
            ins.add(stringBuilder.toString());
        }
        restSession.deleteColumns(ins);
        restSession.closeSession();
    }
}