package cn.edu.tsinghua.iginx.monitor;

import lombok.Data;

@Data
public class NodeResource {

  private double cpu; //剩余核心数
  private double memory; //剩余内存
  private double diskIO; //剩余磁盘占用率
  private double netIO; //剩余带宽流量

  public double getScore() {
    return memory;
  }
}
