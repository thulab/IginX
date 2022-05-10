package cn.edu.tsinghua.iginx.transform.driver;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TransformTaskMeta;
import cn.edu.tsinghua.iginx.transform.api.Driver;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.exception.CreateWorkerException;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.utils.Constants;
import cn.edu.tsinghua.iginx.transform.utils.RedirectLogger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class PythonDriver implements Driver {

    private final IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final static Config config = ConfigDescriptor.getInstance().getConfig();

    private final static Logger logger = LoggerFactory.getLogger(PythonDriver.class);

    private final static String PYTHON_CMD = config.getPythonCMD();

    private final static String PYTHON_DIR = System.getProperty("user.dir");

    private final static String PY_WORKER = File.separator + "python_scripts" + File.separator + "py_worker.py";

    private final static String PY_SUFFIX = ".py";

    private final static int TEST_WAIT_TIME = 10000;

    private static PythonDriver instance;

    private PythonDriver() {
        File file = new File(PYTHON_DIR + PY_WORKER);
        if (!file.exists()) {
            logger.error("Python driver file didn't exists.");
        }
    }

    public static PythonDriver getInstance() {
        if (instance == null) {
            synchronized (PythonDriver.class) {
                if (instance == null) {
                    instance = new PythonDriver();
                }
            }
        }
        return instance;
    }

    @Override
    public Worker createWorker(PythonTask task, Writer writer) throws TransformException {
        String className = task.getClassName();

        TransformTaskMeta taskMeta = metaManager.getTransformTask(className);
        if (taskMeta == null) {
            throw new CreateWorkerException(String.format("Fail to load task info by className: %s", className));
        }
        String fileName = taskMeta.getFileName();
        String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(new byte[]{127, 0, 0, 1}));
            int javaPort = serverSocket.getLocalPort();

            ProcessBuilder pb = new ProcessBuilder();
            pb.inheritIO().command(
                PYTHON_CMD,
                PYTHON_DIR + PY_WORKER,
                moduleName,
                className,
                String.valueOf(javaPort)
            );
            Process process = pb.start();

            // Redirect worker process stdout and stderr
//            redirectStreamsToLogger(process.getInputStream(), process.getErrorStream());

            // Wait for it to connect to our socket.
//            serverSocket.setSoTimeout(TEST_WAIT_TIME);

            Socket socket = serverSocket.accept();

            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            try (ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
                VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
                reader.loadNextBatch();

                BigIntVector pidVector = (BigIntVector) readBatch.getVector(0);
                long pid = pidVector.get(0);
                BigIntVector portVector = (BigIntVector) readBatch.getVector(1);
                int pyPort = (int) portVector.get(0);
                BigIntVector statusVector = (BigIntVector) readBatch.getVector(2);
                int status = (int) statusVector.get(0);

                socket.close();

                if (pid < 0) {
                    throw new CreateWorkerException(String.format("Failed to launch python worker with pid=%d", pid));
                } else if (status < 0) {
                    throw new CreateWorkerException(String.format("Failed to launch python worker with status=%s", Constants.getWorkerStatusInfo(status)));
                } else {
                    Worker worker = new Worker(pid, javaPort, pyPort, process, serverSocket, writer);
                    logger.info(worker.toString() + " has started.");
                    return worker;
                }
            }
        } catch (IOException e) {
            throw new CreateWorkerException("Failed to launch python worker", e);
        }
    }

    public boolean testWorker(String fileName, String className) {
        ServerSocket serverSocket = null;
        Process process = null;
        try {
            serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(new byte[]{127, 0, 0, 1}));
            int javaPort = serverSocket.getLocalPort();
            String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));

            ProcessBuilder pb = new ProcessBuilder();
            pb.inheritIO().command(
                PYTHON_CMD,
                PYTHON_DIR + PY_WORKER,
                moduleName,
                className,
                String.valueOf(javaPort)
            );
            process = pb.start();

            // Redirect worker process stdout and stderr
//            redirectStreamsToLogger(process.getInputStream(), process.getErrorStream());

            // Wait for it to connect to our socket.
            serverSocket.setSoTimeout(TEST_WAIT_TIME);

            Socket socket = serverSocket.accept();
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            try (ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
                VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
                reader.loadNextBatch();

                BigIntVector pidVector = (BigIntVector) readBatch.getVector(0);
                long pid = pidVector.get(0);

                socket.close();

                if (pid < 0) {
                    logger.error(String.format("Failed to launch python worker with code=%d", pid));
                    return false;
                } else {
                    logger.info(String.format("Worker(pid=%d) has started.", pid));
                    return true;
                }
            }
        } catch (IOException e) {
            logger.error("Failed to launch python worker", e);
            return false;
        } finally {
            if (process != null && process.isAlive()) {
                process.destroy();
            }
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    logger.error("Fail to close server socket, because ", e);
                }
            }
        }
    }

    private void redirectStreamsToLogger(InputStream stdout, InputStream stderr) {
        new RedirectLogger(stdout, "stdout").start();
        new RedirectLogger(stderr, "stderr").start();
    }
}
