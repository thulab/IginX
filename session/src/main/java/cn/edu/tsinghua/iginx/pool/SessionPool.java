package cn.edu.tsinghua.iginx.pool;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.*;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

public class SessionPool {
    private static final Logger logger = LoggerFactory.getLogger(SessionPool.class);
    public static final String SESSION_POOL_IS_CLOSED = "Session pool is closed";
    public static final String CLOSE_THE_SESSION_FAILED = "close the session failed.";

    private static final int RETRY = 3;
    private static final int FINAL_RETRY = RETRY - 1;

    private final ConcurrentLinkedDeque<Session> queue = new ConcurrentLinkedDeque<>();
    // for session whose resultSet is not released.
    private final ConcurrentMap<Session, Session> occupied = new ConcurrentHashMap<>();

    private long waitToGetSessionTimeoutInMs;

    private int size = 0;
    private int maxSize = 0;

    private static final String USERNAME = "root";

    private static final String PASSWORD = "root";

    private static final int MAXSIZE = 10;
    private static long WAITTOGETSESSIONTIMEOUTINMS = 60_000;

    // parameters for Session constructor
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    // whether the queue is closed.
    private boolean closed;

    public SessionPool(String host, int port) throws SessionException {
        this(host, port, USERNAME, PASSWORD, MAXSIZE);
    }

    public SessionPool(String host, String port) throws SessionException {
        this(host, port, USERNAME, PASSWORD);
    }

    public SessionPool(String host, String portString, String username, String password) throws SessionException {
        this(host, Integer.parseInt(portString), username, password, MAXSIZE);
    }

    public SessionPool(String host, String portString, String username, String password, int maxsize) throws SessionException {
        this(host, Integer.parseInt(portString), username, password, maxsize);
    }

    public SessionPool(String host, String portString, String username, String password, int maxsize, long waitToGetSessionTimeoutInMs) throws SessionException {
        this(host, Integer.parseInt(portString), username, password, maxsize, waitToGetSessionTimeoutInMs);
    }

    public SessionPool(String host, int port, String user, String password, int maxSize) throws SessionException {
        this(
            host,
            port,
            user,
            password,
            maxSize,
            WAITTOGETSESSIONTIMEOUTINMS);
    }

    public SessionPool(
            String host,
            int port,
            String user,
            String password,
            int maxSize,
            long waitToGetSessionTimeoutInMs) throws SessionException {
        this.maxSize = maxSize;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;

        if (maxSize <=0) {
            logger.error("session pool maxSize value invalid");
            throw new SessionException("session pool maxSize value invalid");
        }
    }

    private Session constructNewSession() {
        Session session;
        // Construct custom Session
        session = new Session(host, port, user, password);
        return session;
    }

    private Session getSession() throws SessionException {
        Session session = queue.poll();
        if (closed) {
            throw new SessionException(SESSION_POOL_IS_CLOSED);
        }
        if (session != null) {
            return session;
        }

        boolean shouldCreate = false;

        long start = System.currentTimeMillis();
        while (session == null) {
            synchronized (this) {
                if (size < maxSize) {
                    // we can create more session
                    size++;
                    shouldCreate = true;
                    // but we do it after skip synchronized block because connection a session is time
                    // consuming.
                    break;
                }

                // we have to wait for someone returns a session.
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("no more sessions can be created, wait... queue.size={}", queue.size());
                    }
                    this.wait(1000);
                    long timeOut = Math.min(waitToGetSessionTimeoutInMs, 60_000);
                    if (System.currentTimeMillis() - start > timeOut) {
                        logger.warn(
                                "the SessionPool has wait for {} seconds to get a new connection: {}:{} with {}, {}",
                                (System.currentTimeMillis() - start) / 1000,
                                host,
                                port,
                                user,
                                password);
                        logger.warn(
                                "current occupied size {}, queue size {}, considered size {} ",
                                occupied.size(),
                                queue.size(),
                                size);
                        if (System.currentTimeMillis() - start > waitToGetSessionTimeoutInMs) {
                            throw new SessionException(
                                    String.format("timeout to get a connection from %s:%s", host, port));
                        }
                    }
                } catch (InterruptedException e) {
                    // wake up from this.wait(1000) by this.notify()
                }

                session = queue.poll();

                if (closed) {
                    throw new SessionException(SESSION_POOL_IS_CLOSED);
                }
            }
        }

        if (shouldCreate) {
            // create a new one.
            if (logger.isDebugEnabled()) {
                logger.debug("Create a new redirect Session {}, {}", user, password);
            }

            session = constructNewSession();

            try {
                session.openSession();
                // avoid someone has called close() the session pool
                synchronized (this) {
                    if (closed) {
                        // have to release the connection...
                        session.closeSession();
                        throw new SessionException(SESSION_POOL_IS_CLOSED);
                    }
                }
            } catch (SessionException e) {
                // if exception, we will throw the exception.
                // Meanwhile, we have to set size--
                synchronized (this) {
                    size--;
                    // we do not need to notifyAll as any waited thread can continue to work after waked up.
                    this.notify();
                    if (logger.isDebugEnabled()) {
                        logger.debug("open session failed, reduce the count and notify others...");
                    }
                }
                throw e;
            }
        }

        return session;
    }

    public int currentAvailableSize() {
        return queue.size();
    }

    public int currentOccupiedSize() {
        return occupied.size();
    }

    private void putBack(Session session) {
        queue.push(session);
        synchronized (this) {
            // we do not need to notifyAll as any waited thread can continue to work after waked up.
            this.notify();
            // comment the following codes as putBack is too frequently called.
            //      if (logger.isTraceEnabled()) {
            //        logger.trace("put a session back and notify others..., queue.size = {}",
            // queue.size());
            //      }
        }
    }

    private void occupy(Session session) {
        occupied.put(session, session);
    }

    private void tryConstructNewSession() {
        Session session = constructNewSession();
        try {
            session.openSession();
            // avoid someone has called close() the session pool
            synchronized (this) {
                if (closed) {
                    // have to release the connection...
                    session.closeSession();
                    throw new SessionException(SESSION_POOL_IS_CLOSED);
                }
                queue.push(session);
                this.notify();
            }
        } catch (SessionException e) {
            synchronized (this) {
                size--;
                // we do not need to notifyAll as any waited thread can continue to work after waked up.
                this.notify();
                if (logger.isDebugEnabled()) {
                    logger.debug("open session failed, reduce the count and notify others...");
                }
            }
        }
    }

    public synchronized void close() throws SessionException {
        for (Session session : queue) {
            try {
                session.closeSession();
            } catch (SessionException e) {
                // do nothing
                logger.warn(CLOSE_THE_SESSION_FAILED, e);
            }
        }
        for (Session session : occupied.keySet()) {
            try {
                session.closeSession();
            } catch (SessionException e) {
                // do nothing
                logger.warn(CLOSE_THE_SESSION_FAILED, e);
            }
        }
        logger.info("closing the session pool, cleaning queues...");
        this.closed = true;
        queue.clear();
        occupied.clear();
    }

    private void closeSession(Session session) {
        if (session != null) {
            try {
                session.closeSession();
            } catch (Exception e2) {
                // do nothing. We just want to guarantee the session is closed.
                logger.warn(CLOSE_THE_SESSION_FAILED, e2);
            }
        }
    }

    public void addStorageEngine(String ip, int port, String type, Map<String, String> extraParams) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.addStorageEngine(ip, port, type, extraParams);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("addStorageEngine failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void addStorageEngines(List<StorageEngine> storageEngines) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.addStorageEngines(storageEngines);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("addStorageEngines failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    private void cleanSessionAndMayThrowConnectionException(
            Session session, int times, SessionException e) throws SessionException {
        closeSession(session);
        tryConstructNewSession();
        if (times == FINAL_RETRY) {
            throw new SessionException(
                    String.format(
                            "retry to execute statement on %s:%s failed %d times: %s",
                            host, port, RETRY, e.getMessage()),
                    e);
        }
    }

    public List<Column> showColumns() throws SessionException, ExecutionException {
        List<Column> ret = new ArrayList<>();
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                ret = session.showColumns();
                putBack(session);
                return ret;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertTablet failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return ret;
    }

    public void deleteColumn(String path) throws SessionException,
            ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.deleteColumn(path);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("deleteColumn failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void deleteColumns(List<String> paths) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.deleteColumns(paths);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("deleteColumns failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                    List<DataType> dataTypeList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertColumnRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                    List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertColumnRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                    List<DataType> dataTypeList, List<Map<String, String>> tagsList, TimePrecision precision) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList, precision);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertColumnRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertNonAlignedColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertNonAlignedColumnRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertNonAlignedColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertNonAlignedColumnRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertNonAlignedColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList, List<Map<String, String>> tagsList, TimePrecision precision)
            throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList, precision);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertNonAlignedColumnRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                 List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertRowRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                 List<DataType> dataTypeList, List<Map<String, String>> tagsList, TimePrecision precison) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList, precison);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertRowRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertNonAlignedRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                           List<DataType> dataTypeList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertNonAlignedRowRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertNonAlignedRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                           List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertNonAlignedRowRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void insertNonAlignedRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                           List<DataType> dataTypeList, List<Map<String, String>> tagsList, TimePrecision precision)
            throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList, precision);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("insertNonAlignedRowRecords failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void deleteDataInColumn(String path, long startTime, long endTime) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.deleteDataInColumn(path, startTime, endTime);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("deleteDataInColumn failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void deleteDataInColumns(List<String> paths, long startTime, long endTime) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.deleteDataInColumns(paths, startTime, endTime);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("deleteDataInColumns failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void deleteDataInColumns(List<String> paths, long startTime, long endTime, Map<String, List<String>> tagsList) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.deleteDataInColumns(paths, startTime, endTime, tagsList);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("deleteDataInColumns failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime)
            throws SessionException, ExecutionException {
        SessionQueryDataSet sessionQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionQueryDataSet = session.queryData(paths, startTime, endTime);
                putBack(session);
                return sessionQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("queryData failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionQueryDataSet;
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime, Map<String, List<String>> tagsList)
            throws SessionException, ExecutionException {
        SessionQueryDataSet sessionQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionQueryDataSet = session.queryData(paths, startTime, endTime, tagsList);
                putBack(session);
                return sessionQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("queryData failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionQueryDataSet;
    }

    public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType)
            throws SessionException, ExecutionException {
        SessionAggregateQueryDataSet sessionAggregateQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionAggregateQueryDataSet = session.aggregateQuery(paths, startTime, endTime, aggregateType);
                putBack(session);
                return sessionAggregateQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("aggregateQuery failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionAggregateQueryDataSet;
    }

    public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType, Map<String, List<String>> tagsList)
            throws SessionException, ExecutionException {
        SessionAggregateQueryDataSet sessionAggregateQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionAggregateQueryDataSet = session.aggregateQuery(paths, startTime, endTime, aggregateType);
                putBack(session);
                return sessionAggregateQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("aggregateQuery failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionAggregateQueryDataSet;
    }

    public SessionQueryDataSet downsampleQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision)
            throws SessionException, ExecutionException {
        SessionQueryDataSet sessionQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionQueryDataSet = session.downsampleQuery(paths, startTime, endTime, aggregateType, precision);
                putBack(session);
                return sessionQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("downsampleQuery failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionQueryDataSet;
    }

    public SessionQueryDataSet downsampleQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision, Map<String, List<String>> tagsList)
            throws SessionException, ExecutionException {
        SessionQueryDataSet sessionQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionQueryDataSet = session.downsampleQuery(paths, startTime, endTime, aggregateType, precision, tagsList);
                putBack(session);
                return sessionQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("downsampleQuery failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionQueryDataSet;
    }

    public int getReplicaNum() throws SessionException, ExecutionException {
        int ret = 0;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                ret = session.getReplicaNum();
                putBack(session);
                return ret;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("getReplicaNum failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return ret;
    }

    public SessionExecuteSqlResult executeSql(String statement) throws SessionException, ExecutionException {
        SessionExecuteSqlResult sessionExecuteSqlResult= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionExecuteSqlResult = session.executeSql(statement);
                putBack(session);
                return sessionExecuteSqlResult;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("executeSql failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionExecuteSqlResult;
    }

    public SessionQueryDataSet queryLast(List<String> paths, long startTime)
            throws SessionException, ExecutionException {
        SessionQueryDataSet sessionQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionQueryDataSet = session.queryLast(paths, startTime);
                putBack(session);
                return sessionQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("queryLast failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionQueryDataSet;
    }

    public SessionQueryDataSet queryLast(List<String> paths, long startTime, Map<String, List<String>> tagsList)
            throws SessionException, ExecutionException {
        SessionQueryDataSet sessionQueryDataSet= null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                sessionQueryDataSet = session.queryLast(paths, startTime, tagsList);
                putBack(session);
                return sessionQueryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("queryLast failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return sessionQueryDataSet;
    }

    public void addUser(String username, String password, Set<AuthType> auths) throws SessionException, ExecutionException  {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.addUser(username, password, auths);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("addUser failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void updateUser(String username, String password, Set<AuthType> auths) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.updateUser(username, password, auths);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("updateUser failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public void deleteUser(String username) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.deleteUser(username);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("deleteUser failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public ClusterInfo getClusterInfo() throws SessionException, ExecutionException {
        ClusterInfo clusterInfo = null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                clusterInfo = session.getClusterInfo();
                putBack(session);
                return clusterInfo;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("getClusterInfo failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return clusterInfo;
    }

    public QueryDataSet executeQuery(String statement) throws SessionException, ExecutionException {
        QueryDataSet queryDataSet = null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                queryDataSet = session.executeQuery(statement);
                putBack(session);
                return queryDataSet;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("executeQuery failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return queryDataSet;
    }

    public long commitTransformJob(List<TaskInfo> taskInfoList, ExportType exportType,
                                   String fileName) throws SessionException, ExecutionException {
        long ret = 0;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                ret = session.commitTransformJob(taskInfoList, exportType, fileName);
                putBack(session);
                return ret;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("commitTransformJob failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return ret;
    }

    public JobState queryTransformJobStatus(long jobId) throws SessionException, ExecutionException {
        JobState ret = null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                ret = session.queryTransformJobStatus(jobId);
                putBack(session);
                return ret;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("queryTransformJobStatus failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return ret;
    }

    public List<Long> showEligibleJob(JobState jobState) throws SessionException, ExecutionException {
        List<Long> ret = null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                ret = session.showEligibleJob(jobState);
                putBack(session);
                return ret;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("showEligibleJob failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return ret;
    }

    public void cancelTransformJob(long jobId) throws SessionException, ExecutionException {
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                session.cancelTransformJob(jobId);
                putBack(session);
                return;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("cancelTransformJob failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
    }

    public CurveMatchResult curveMatch(List<String> paths, long startTime, long endTime, List<Double> curveQuery, long curveUnit)
            throws SessionException, ExecutionException {
        CurveMatchResult ret = null;
        for (int i = 0; i < RETRY; i++) {
            Session session = getSession();
            try {
                ret = session.curveMatch(paths, startTime, endTime, curveQuery, curveUnit);
                putBack(session);
                return ret;
            } catch (SessionException e) {
                // TException means the connection is broken, remove it and get a new one.
                logger.warn("curveMatch failed", e);
                cleanSessionAndMayThrowConnectionException(session, i, e);
            } catch (ExecutionException | RuntimeException e) {
                putBack(session);
                throw e;
            }
        }
        return ret;
    }


    public static class Builder {
        private String host;
        private int port;
        private int maxSize;
        private String user;
        private String password;
        private int fetchSize;
        private long waitToGetSessionTimeoutInMs = 60_000;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder maxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
            this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public SessionPool build() throws SessionException {
            return new SessionPool(
                    host,
                    port,
                    user,
                    password,
                    maxSize,
                    fetchSize
            );
        }
    }

}
