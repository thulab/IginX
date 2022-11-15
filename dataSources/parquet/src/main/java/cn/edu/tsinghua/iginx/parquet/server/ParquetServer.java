package cn.edu.tsinghua.iginx.parquet.server;

import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.parquet.thrift.ParquetService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetServer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ParquetServer.class);

    private final int port;

    private final Executor executor;

    public ParquetServer(int port, Executor executor) {
        this.port = port;
        this.executor = executor;
    }

    private void startServer() throws TTransportException {
        TProcessor processor = new ParquetService.Processor<ParquetService.Iface>(new ParquetWorker(executor));
        TServerSocket serverTransport = new TServerSocket(port);
        TThreadPoolServer.Args args = new TThreadPoolServer
            .Args(serverTransport)
            .processor(processor)
            .minWorkerThreads(20);
        args.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TThreadPoolServer(args);
        logger.info("parquet service starts successfully!");
        server.serve();
    }

    @Override
    public void run() {
        try {
            startServer();
        } catch (TTransportException e) {
            logger.error("parquet service starts failure.");
        }
    }
}
