package cn.edu.tsinghua.iginx.transform.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mutex {

    private boolean isLocked = false;

    private final static Logger logger = LoggerFactory.getLogger(Mutex.class);

    public synchronized void lock() {
        while (this.isLocked) {
            try {
                wait();
            } catch (InterruptedException e) {
                logger.error("Mutex was interrupted");
            }
        }
        this.isLocked = true;
    }

    public synchronized void unlock() {
        this.isLocked = false;
        this.notify();
    }
}
