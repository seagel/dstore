package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Executor {
    ZooKeeper zooKeeper;
    final CountDownLatch connectionLatch = new CountDownLatch(1);

    public Executor(String hosts){
        try {
            zooKeeper = new ZooKeeper(hosts, 100, we -> {
                if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            });
            connectionLatch.await();
            if(zooKeeper.exists("/services",false) == null)
                zooKeeper.create("/services" ,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (IOException | KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

}
