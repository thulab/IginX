package cn.edu.tsinghua.iginx.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class RestServer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestServer.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private static URI baseURI;

    private static URI getBaseURI()
    {
        return UriBuilder.fromUri("http://" + config.getRestip() + "/").port(config.getRestport()).build();
    }

    private static HttpServer startServer()
    {
        config = ConfigDescriptor.getInstance().getConfig();
        baseURI = getBaseURI();
        final ResourceConfig rc = new ResourceConfig().packages("cn.edu.tsinghua.iginx.rest");
        return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
    }


    public static void main(String[] argv)
    {
        HttpServer server = null;
        try
        {
            server = startServer();
        }
        catch (Exception e)
        {
            LOGGER.error("启动Rest服务失败，请检查是否启动了IoTDB服务以及相关配置参数是否正确", e);
            System.exit(1);
        }
        LOGGER.info("Iginx REST server has been available at {}.", baseURI);
        try
        {
            Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Rest主线程出现异常", e);
            Thread.currentThread().interrupt();
        }
        server.shutdown();
    }
}
