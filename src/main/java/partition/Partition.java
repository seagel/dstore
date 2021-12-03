package partition;

import model.Transaction;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import constants.ConfigurableConstants;

import java.io.*;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static java.lang.Math.abs;


public class Partition extends Thread {

    int id,itr;
    
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
        processingQueue = new ArrayBlockingQueue<>(1000);
        offset = 0;
        Range r = new Range(0,0);
        this.read_set = r;
    }

    public void pushToProduceQueue() {
        int x =100;
        Transaction curr = generateTransaction();
        curr.setOriginatorPartition(id);
//        while (x > 0) {
        if(id < 3)
            processingQueue.add(curr);
//            x--;
//        }
    }

    private Transaction generateTransaction(){
        Set<Integer> readSet = new HashSet<>();
        Random random = new Random();
//        readSet.add(abs(random.nextInt()) % 5);
//        readSet.add(abs(random.nextInt()) % 5);
//        readSet.add(3);
//        readSet.add(4);
//        readSet.add(5);
        Set<Integer> writeSet = new HashSet<>();
//        w.add(1);
        writeSet.add(abs(random.nextInt() % 1000000));
//        writeSet.add(abs(random.nextInt() % 5));
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
//        while (true) {
            try {
            	this.itr = 0;
                children = zooKeeper.getChildren("/sequencer", false);
                this.read_set.start = this.read_set.end+1;
                int s_index = this.read_set.end+1, e_index ;
                if(children.size() >= this.read_set.start + ConfigurableConstants.ZOOKEEPER_BATCH_READ_SIZE)
                {
                	e_index = this.read_set.start + ConfigurableConstants.ZOOKEEPER_BATCH_READ_SIZE;
                }
                else
                {
                	e_index = children.size();
                }
                
                children.subList(s_index, e_index).forEach(x -> {
                	
                	this.itr++;
                	this.read_set.end = this.read_set.start+this.itr;
                	System.out.println("index read = "+this.read_set.end);
                	
                //children.stream().limit(10).forEach(x -> {
                    try {
                        Transaction curr = (Transaction) getObject(zooKeeper.getData("/sequencer/"+x, false, zooKeeper.exists("/sequencer/"+x, false)));
//                        curr.print();
                        txnProcessor.processTransaction(curr);

                    } catch (KeeperException | InterruptedException | IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
//                    System.out.println("Partition id : " + this.getId() + " ...." + x);
                });//printing the order of the transactions in every thread
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
//            break;
//        }
    }

    private void startWritingToLog() {
        ZooKeeper zooKeeper = initializer.executor.getZooKeeper();
        Transaction curr;
        while (true) {
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

    public Range getRange() {
        return range;
    }


}
