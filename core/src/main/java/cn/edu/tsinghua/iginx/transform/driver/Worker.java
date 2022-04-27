package cn.edu.tsinghua.iginx.transform.driver;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.ArrowReader;
import cn.edu.tsinghua.iginx.transform.data.BatchData;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker extends Thread {

    private final long pid;

    private final String ip;

    private final int javaPort;

    private final int pyPort;

    private final Process process;

    private final ServerSocket serverSocket;

    private final Writer writer;

    private final static Logger logger = LoggerFactory.getLogger(Worker.class);

    private final static Config config = ConfigDescriptor.getInstance().getConfig();

    private final ExecutorService threadPool = Executors.newFixedThreadPool(5);

    public Worker(long pid, int javaPort, int pyPort, Process process, ServerSocket serverSocket, Writer writer) {
        this.pid = pid;
        this.ip = config.getIp();
        this.javaPort = javaPort;
        this.pyPort = pyPort;
        this.process = process;
        this.serverSocket = serverSocket;
        this.writer = writer;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                threadPool.submit(() -> process(socket));
            }
        } catch (SocketException ignored) {
            logger.info(toString() + " stop server socket.");
        } catch (IOException e) {
            throw new RuntimeException("An error occurred while listening.", e);
        }
    }

    public void process(Socket socket) {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try (ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
            reader.loadNextBatch();

            Reader arrowReader = new ArrowReader(readBatch, config.getBatchSize());
            while (arrowReader.hasNextBatch()) {
                BatchData batchData = arrowReader.loadNextBatch();
                writer.writeBatch(batchData);
            }

            reader.close();
            socket.close();
        } catch (IOException | WriteBatchException e) {
            logger.error(String.format("Worker pid=%d fail to process socket.", pid));
            throw new RuntimeException("Fail to process socket", e);
        }
    }

    @Override
    public void destroy() {
        if (process.isAlive()) {
            this.process.destroy();
        }
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                this.serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        }
        threadPool.shutdown();
    }

    public long getPid() {
        return pid;
    }

    public int getPyPort() {
        return pyPort;
    }

    public Process getProcess() {
        return process;
    }

    @Override
    public String toString() {
        return "Worker{" +
            "pid=" + pid +
            ", ip='" + ip + '\'' +
            ", javaPort=" + javaPort +
            ", pyPort=" + pyPort +
            ", process=" + process +
            ", serverSocket=" + serverSocket +
            ", writer=" + writer +
            ", threadPool=" + threadPool +
            '}';
    }
}
