package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.thrift.IService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Iginx {

    private static final Logger logger = LoggerFactory.getLogger(Iginx.class);

    public static void main(String[] args) throws Exception {
        Iginx iginx = new Iginx();
        iginx.startServer();
    }

    private void startServer() throws TTransportException {
        TProcessor processor = new IService.Processor<IService.Iface>(IginxWorker.getInstance());
        TServerSocket serverTransport = new TServerSocket(ConfigDescriptor.getInstance().getConfig().getPort());
        TServer.Args args = new TServer.Args(serverTransport);
        args.processor(processor);
        args.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TSimpleServer(args);
        server.serve();
    }


}
