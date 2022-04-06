package cn.edu.tsinghua.iginx.monitor;

import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeResourceMonitor implements IMonitor {

  private static final Logger logger = LoggerFactory.getLogger(NodeResourceMonitor.class);

  private static NodeResource nodeResource;

  private Thread thread;
  private static final NodeResourceMonitor instance = new NodeResourceMonitor();
  private static final Sigar sigar = new Sigar();

  public static NodeResourceMonitor getInstance() {
    return instance;
  }

  @Override
  public void start() {
    thread = new Thread(new NodeResourceThread());
    thread.start();
  }

  @Override
  public void stop() {
    thread.interrupt();
    thread = null;
  }

  public NodeResource getNodeResource() {
    return nodeResource;
  }

  private static class NodeResourceThread implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(1000);
          this.updateCPU();
          this.updateMemory();
          this.updateDiskIO();
          this.updateNetIO();
        } catch (Exception e) {
          logger.error("node resource monitor error", e);
        }
      }
    }

    private void updateCPU() {
      //TODO
    }

    private void updateMemory() throws SigarException {
      Mem mem = sigar.getMem();
      double usedMem = mem.getUsedPercent();
      nodeResource.setMemory(usedMem);
    }

    private void updateDiskIO() {
      //TODO
    }

    private void updateNetIO() {
      //TODO
    }
  }
}