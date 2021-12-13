package partition;

import jdk.jfr.EventType;
import model.Transaction;

import org.apache.zookeeper.*;

import constants.ConfigurableConstants;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;


public class Partition extends Thread { //partition analogous to thread

    int id, itr;

    Range range;
    Initializer initializer;
    ArrayBlockingQueue<Transaction> processingQueue;
    Map <String,Boolean> readTransactions;
    TxnProcessor txnProcessor;

    public Partition(int id, Initializer initializer, Range range) {
        this.id = id;
        this.range = range;
        this.initializer = initializer;
        processingQueue = new ArrayBlockingQueue<>(ConfigurableConstants.PROCESSING_QUEUE_SIZE);
        this.readTransactions = new HashMap<>();
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

                children = zooKeeper.getChildren("/orderer", false);
                Collections.sort(children);
//                    System.out.println(s_index + "....." + e_index);
                    children.stream().filter(x -> !readTransactions.containsKey(x)).forEach(x -> {
                        try {
                            Transaction curr = (Transaction) getObject(zooKeeper.getData("/orderer/" + x, false, zooKeeper.exists("/orderer/" + x, false)));
                            curr.settId(x);
//                            System.out.println("Adding :" + curr.getTransactionId());
                            readTransactions.put(x,true);
                            txnProcessor.processTransaction(curr);
                        } catch (KeeperException | InterruptedException | IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }
//                    System.out.println("Partition id : " + this.getId() + " ...." + x);
                    });
                }catch (KeeperException | InterruptedException e) {
//                e.printStackTrace();
            }
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

    public void setStartTimestamp(long startTimestamp){
        txnProcessor.setStartTimestamp(startTimestamp);
    }


    public void setTotalTxns(long count){
        txnProcessor.setTotalTransactions(count);
    }



}
