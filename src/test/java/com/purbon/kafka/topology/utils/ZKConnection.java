package com.purbon.kafka.topology.utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZKConnection {
  private ZooKeeper zoo;
  CountDownLatch connectionLatch = new CountDownLatch(1);

  public ZooKeeper connect(String host) throws IOException, InterruptedException {
    zoo =
        new ZooKeeper(
            host,
            2000,
            we -> {
              if (we.getState() == KeeperState.SyncConnected) {
                connectionLatch.countDown();
              }
            });

    connectionLatch.await();
    return zoo;
  }

  public void close() throws InterruptedException {
    zoo.close();
  }
}
