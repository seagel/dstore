package partition;

import model.Transaction;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import constants.ConfigurableConstants;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;


public class Partition extends Thread { //partition analogous to thread

    int id, itr;

    Range range;
    Initializer initializer;
    ArrayBlockingQueue<Transaction> processingQueue;
    Range read_set;

    TxnProcessor txnProcessor;
    long offset;

    public Partition(int id, Initializer initializer, Range range) {
        this.id = id;
        this.range = range;
        this.initializer = initializer;
        processingQueue = new ArrayBlockingQueue<>(ConfigurableConstants.PROCESSING_QUEUE_SIZE);
        offset = 0;
        this.read_set = new Range(-1, -1);
    }

    public Range getRange() {
        return this.range;
    }

    public void pushToProduceQueue(List<Transaction> txnList) {
//        System.out.println("Partition :" + id  + " length : " + txnList.size());
        txnList.forEach(txn -> {
            try {
                processingQueue.put(txn);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public void addToProduceQueue(Collection<Transaction> c) {
        processingQueue.addAll(c);
    }

//    private Transaction generateTransaction(){
//        Set<Integer> readSet = new HashSet<>();
//        Random random = new Random();
////        readSet.add(abs(random.nextInt()) % 5);
////        readSet.add(abs(random.nextInt()) % 5);
////        readSet.add(3);
////        readSet.add(4);
////        readSet.add(5);
//        Set<Integer> writeSet = new HashSet<>();
////        w.add(1);
//        writeSet.add(2);
////        writeSet.add(abs(random.nextInt() % 5));
////        readSet.add(4);
////        readSet.add(5);
//        return new Transaction(readSet,writeSet,true);
//    }

    @Override
    public void run() {
//        System.out.println("Starting processing transactions : Partition ->" + id);
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        try {
            if (zooKeeper.exists("/orderer", false) == null) {
                zooKeeper.create("/orderer", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            new Thread(this::startWritingToLog).start();

        } catch (KeeperException | InterruptedException e) {
//            e.printStackTrace();
        }
    }

    private void startReadingFromLog() {
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        List<String> children;
        while (true) {
            try {
                this.itr = 0;
                children = zooKeeper.getChildren("/orderer", false);
                Collections.sort(children);
                this.read_set.start = this.read_set.end + 1;
                int s_index = this.read_set.end + 1, e_index;
                e_index = Math.min(children.size(), this.read_set.start + ConfigurableConstants.ZOOKEEPER_BATCH_READ_SIZE);
                if (s_index <= e_index) {
                    children.subList(s_index, e_index).forEach(x -> {
                        this.itr++;
                        this.read_set.end = this.read_set.start + this.itr;
//                        System.out.println("index read = " + this.read_set.end);

                        //children.stream().limit(10).forEach(x -> {
                        try {
//                            System.out.println("Transaction  " + x);
                            Transaction curr = (Transaction) getObject(zooKeeper.getData("/orderer/" + x, false, zooKeeper.exists("/orderer/" + x, false)));
                            curr.settId(x);
//                        curr.print();
                            txnProcessor.processTransaction(curr);
                        } catch (KeeperException | InterruptedException | IOException | ClassNotFoundException e) {
//                            e.printStackTrace();
                        }
//                    System.out.println("Partition id : " + this.getId() + " ...." + x);
                    });
                }
                //printing the order of the transactions in every thread
            } catch (KeeperException | InterruptedException e) {
//                e.printStackTrace();
            }
//            break;
        }
    }

    private void startWritingToLog() {
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        Transaction curr;
        while (true) {
            try {
                curr = processingQueue.take();
                zooKeeper.create("/orderer/T", getByteArray(curr), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (InterruptedException | KeeperException | IOException e) {
//                e.printStackTrace();
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
