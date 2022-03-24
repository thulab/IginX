package cn.edu.tsinghua.iginx.monitor;

public class NodeResourceMonitor implements IMonitor {

  private NodeResource nodeResource;

  private Thread thread;
  private static final NodeResourceMonitor instance = new NodeResourceMonitor();

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

  public double getScore() {
    return 0;
  }

  private static class NodeResourceThread implements Runnable {

    @Override
    public void run() {

    }
  }
}