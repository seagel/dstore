package Tests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import model.Transaction;
import partition.Initializer;
import zookeeper.Executor;

public class PushDataToZookeeper extends Thread {
	public Executor executor;
	
	public PushDataToZookeeper(Executor executor)
	{
		this.executor = executor;
	}
	private void startWritingToLog(String type) {
        ZooKeeper zooKeeper = executor.getZooKeeper();
        Transaction txn = null ;//= getTransaction();
        while (true) {
            try {
                zooKeeper.create("/sequencer/T",getByteArray(txn) , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (InterruptedException | KeeperException | IOException e) {
                e.printStackTrace();
            }
        }
    }
	
	
	private static byte[] getByteArray(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(obj);
        }
        return bos.toByteArray();
    }

}
