package cn.edu.tsinghua.iginx.parquet.entity;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuckDBConnPool {

    private static final Logger logger = LoggerFactory.getLogger(DuckDBConnPool.class);

    private final ConcurrentLinkedDeque<Connection> queue = new ConcurrentLinkedDeque<>();

    private final DuckDBConnection duckDBConnection;

    private int size;

    private final int maxSize;

    private static final int DEFAULT_MAX_SIZE = 100;

    private static final long WAIT_TIME = 1000;

    private static final long MAX_WAIT_TIME = 30_000;

    private boolean closed = false;

    public DuckDBConnPool(DuckDBConnection conn) {
        this(conn, DEFAULT_MAX_SIZE);
    }

    public DuckDBConnPool(DuckDBConnection conn, int maxSize) {
        this.duckDBConnection = conn;
        this.size = 0;
        this.maxSize = maxSize;
    }

    public Connection getConnection() throws SQLException {
        Connection conn = queue.poll();
        if (closed) {
            throw new SQLException("DuckDB connection pool is closed.");
        }
        if (conn != null) {
            return conn;
        }

        boolean canCreated = false;
        synchronized (this) {
            if (size < maxSize) {
                size++;
                canCreated = true;
            }
        }

        if (canCreated) {
            try {
                logger.info("create a new connection.");
                conn = constructNewConnection();
                synchronized (this) {
                    if (closed) {
                        conn.close();
                        throw new SQLException("DuckDB connection pool is closed.");
                    }
                }
            } catch (SQLException e) {
                synchronized (this) {
                    size--;
                    this.notify();
                    logger.error("create new connection failed, reduce the count and notify others...");
                }
                throw e;
            }
        } else {
            long startTime = System.currentTimeMillis();
            while (conn == null) {
                synchronized (this) {
                    if (closed) {
                        throw new SQLException("DuckDB connection pool is closed.");
                    }

                    try {
                        this.wait(WAIT_TIME);
                        if (System.currentTimeMillis() - startTime > MAX_WAIT_TIME) {
                            logger.error("timeout to get a connection");
                            throw new SQLException("timeout to get a connection");
                        }
                    } catch (InterruptedException e) {
                        logger.error("the connection pool is damaged", e);
                        Thread.currentThread().interrupt();
                    }
                    conn = queue.poll();
                }
            }
        }
        return conn;
    }

    private Connection constructNewConnection() throws SQLException {
        return duckDBConnection.duplicate();
    }

    public void pushBack(Connection conn) {
        if (closed) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("connection close failed");
            }
        } else {
            queue.offer(conn);
            synchronized (this) {
                this.notify();
            }
        }
    }

    public void close() throws SQLException {
        try {
            for (Connection conn : queue) {
                conn.close();
            }
            duckDBConnection.close();
        } catch (SQLException e) {
            logger.error("connection close failed");
            throw e;
        }

        logger.info("closing the connection pool, cleaning queues...");

        this.closed = true;
        queue.clear();
    }
}
