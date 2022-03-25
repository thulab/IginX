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
package cn.edu.tsinghua.iginx.rest.insert;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

public class InsertWorker extends Thread {

  private static final String NO_CACHE = "no-cache";
  private final HttpHeaders httpheaders;
  private InputStream stream;
  private final AsyncResponse asyncResponse;
  private final boolean isAnnotation;

  public InsertWorker(final AsyncResponse asyncResponse, HttpHeaders httpheaders,
      InputStream stream, boolean isAnnotation) {
    this.asyncResponse = asyncResponse;
    this.httpheaders = httpheaders;
    this.stream = stream;
    this.isAnnotation = isAnnotation;
  }

  static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder) {
    responseBuilder.header("Access-Control-Allow-Origin", "*");
    responseBuilder.header("Pragma", NO_CACHE);
    responseBuilder.header("Cache-Control", NO_CACHE);
    responseBuilder.header("Expires", 0);
    return responseBuilder;
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
      DataPointsParser parser = new DataPointsParser(
          new InputStreamReader(stream, StandardCharsets.UTF_8));
      parser.parse(isAnnotation);
      response = Response.status(Response.Status.OK).build();
    } catch (Exception e) {
      response = setHeaders(
          Response.status(Response.Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
    asyncResponse.resume(response);
  }
}
