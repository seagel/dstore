package partition;

import model.Transaction;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;


public class Partition extends Thread {

    int id;
    Range range;
    Initializer initializer;
    ArrayBlockingQueue<Transaction> processingQueue;

    TxnProcessor txnProcessor;
    long offset;

    public Partition(int id, Initializer initializer, Range range) {
        this.id = id;
        this.range = range;
        this.initializer = initializer;
        processingQueue = new ArrayBlockingQueue<>(1000);
        offset = 0;
    }

    public void pushToProduceQueue() {
        int x =100;
        Transaction curr = generateTransaction();
        curr.setOriginatorPartition(id);
//        while (x > 0) {
            processingQueue.add(curr);
//            x--;
//        }
    }

    private Transaction generateTransaction(){
        Set<Integer> readSet = new HashSet<>();
        readSet.add(1);
//        readSet.add(2);
//        readSet.add(3);
//        readSet.add(4);
//        readSet.add(5);
        Set<Integer> writeSet = new HashSet<>();
//        w.add(1);
//        readSet.add(2);
//        readSet.add(3);
//        readSet.add(4);
//        readSet.add(5);
        return new Transaction(readSet,writeSet,true);
    }

    @Override
    public void run() {
        System.out.println("Starting processing transactions : Partition ->" + id);
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        try {
            if (zooKeeper.exists("/sequencer", false) == null) {
                zooKeeper.create("/sequencer", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            new Thread(this::startWritingToLog).start();

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void startReadingFromLog() {
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        List<String> children;
        while (true) {
            try {
                children = zooKeeper.getChildren("/sequencer", false);
                children.stream().limit(10).forEach(x -> {
                    try {
                        Transaction curr = (Transaction) getObject(zooKeeper.getData("/sequencer/"+x, false, zooKeeper.exists("/sequencer/"+x, false)));
//                        curr.print();
                        txnProcessor.processTransaction(curr);
                    } catch (KeeperException | InterruptedException | IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Partition id : " + this.getId() + " ...." + x);
                });//printing the order of the transactions in every thread
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            break;
        }


    }

    private void startWritingToLog() {
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        Transaction curr;
        while (true) {
            ByteArrayOutputStream baos=new ByteArrayOutputStream();
            try {
                curr = processingQueue.take();
                zooKeeper.create("/sequencer/T",getByteArray(curr) , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (InterruptedException | KeeperException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void setTxnProcessor(TxnProcessor txnProcessor) {
        this.txnProcessor = txnProcessor;
        new Thread(this::startReadingFromLog).start();
    }

    private static byte[] getByteArray(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(obj);
        }
        return bos.toByteArray();
    }

    /* De serialize the byte array to object */
    private static Object getObject(byte[] byteArr) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArr);
        ObjectInput in = new ObjectInputStream(bis);
        return in.readObject();
    }


}
