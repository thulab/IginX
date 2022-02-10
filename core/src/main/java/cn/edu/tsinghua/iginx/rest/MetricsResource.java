/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.insert.InsertWorker;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Path("/")
public class MetricsResource {

    private static final String INSERT_URL = "api/v1/datapoints";
    private static final String INSERT_ANNOTATION_URL = "api/v1/datapoints/annotations";
    private static final String QUERY_URL = "api/v1/datapoints/query";
    private static final String QUERY_ANNOTATION_URL = "api/v1/datapoints/query/annotations";
    private static final String DELETE_URL = "api/v1/datapoints/delete";
    private static final String DELETE_METRIC_URL = "api/v1/metric/{metricName}";
    private static final String GRAFANA_OK = "";
    private static final String GRAFANA_QUERY = "query";
    private static final String GRAFANA_STRING = "annotations";
    private static final String ERROR_PATH = "{string : .+}";

    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(config.getAsyncRestThreadPool());
    private final IMetaManager metaManager = DefaultMetaManager.getInstance();

    @Inject
    public MetricsResource() {
    }

    @GET
    @Path(GRAFANA_OK)
    public Response OK() {
        return setHeaders(Response.status(Status.OK)).build();
    }

    @POST
    @Path(GRAFANA_QUERY)
    public Response grafanaQuery(String jsonStr) {
        try {
            if (jsonStr == null) {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = parser.parseGrafanaQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(false);
            String entity = parser.parseResultToGrafanaJson(result);
            return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();

        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    @POST
    @Path(INSERT_ANNOTATION_URL)
    public void addAnnotation(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse) {
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream, true));
    }

    @POST
    @Path(GRAFANA_STRING)
    public Response grafanaAnnotation(String jsonStr) {
        try {
            return postQuery(jsonStr, true, true);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    @POST
    @Path(QUERY_ANNOTATION_URL)
    public Response queryAnnotation(String jsonStr) {

        try {
            return postQuery(jsonStr, true, false);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }


    @POST
    @Path(ERROR_PATH)
    public Response postErrorPath(@PathParam("string") String str) {
        return setHeaders(Response.status(Status.NOT_FOUND).entity("Wrong path\n")).build();
    }

    @GET
    @Path(ERROR_PATH)
    public Response getErrorPath(@PathParam("string") String str) {
        return setHeaders(Response.status(Status.NOT_FOUND).entity("Wrong path\n")).build();
    }

    @POST
    @Path(INSERT_URL)
    public void add(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse) {
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream, false));
    }

    @POST
    @Path(QUERY_URL)
    public Response postQuery(final InputStream stream) {
        try {
            String str = inputStreamToString(stream);
            return postQuery(str, false, false);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }


    @POST
    @Path(DELETE_URL)
    public Response postDelete(final InputStream stream) {
        try {
            String str = inputStreamToString(stream);
            return postDelete(str);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    @DELETE
    @Path(DELETE_METRIC_URL)
    public Response metricDelete(@PathParam("metricName") String metricName) {
        try {
            deleteMetric(metricName);
            return setHeaders(Response.status(Status.OK)).build();
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder) {
        responseBuilder.header("Access-Control-Allow-Origin", "*");
        responseBuilder.header("Access-Control-Allow-Methods", "POST");
        responseBuilder.header("Access-Control-Allow-Headers", "accept, content-type");
        responseBuilder.header("Pragma", "no-cache");
        responseBuilder.header("Cache-Control", "no-cache");
        responseBuilder.header("Expires", 0);
        return (responseBuilder);
    }

    private static String inputStreamToString(InputStream inputStream) throws Exception {
        StringBuilder buffer = new StringBuilder();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String str;
        while ((str = bufferedReader.readLine()) != null) {
            buffer.append(str);
        }
        bufferedReader.close();
        inputStreamReader.close();
        inputStream.close();
        return buffer.toString();
    }

    public Response postQuery(String jsonStr, boolean isAnnotation, boolean isGrafana) {
        try {
            if (jsonStr == null) {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = isAnnotation ? parser.parseAnnotationQueryMetric(jsonStr, isGrafana) : parser.parseQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(false);
            String entity = isAnnotation ? parser.parseResultToAnnotationJson(result, isGrafana) : parser.parseResultToJson(result, false);
            return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();

        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    public Response postDelete(String jsonStr) {
        try {
            if (jsonStr == null) {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(true);
            String entity = parser.parseResultToJson(result, true);
            return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    void deleteMetric(String metricName) throws Exception {
        RestSession restSession = new RestSession();
        restSession.openSession();
        List<String> ins = new ArrayList<>();
        for (int i = 1; i < ConfigDescriptor.getInstance().getConfig().getMaxTimeseriesLength(); i++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 0; j < i; j++) {
                stringBuilder.append("*.");
            }
            stringBuilder.append(metricName);
            ins.add(stringBuilder.toString());
        }
        metaManager.addOrUpdateSchemaMapping(metricName, null);
        restSession.deleteColumns(ins);
        restSession.closeSession();
    }
}