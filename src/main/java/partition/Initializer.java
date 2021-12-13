package partition;

import model.Transaction;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.*;
import storage.Store;
import zookeeper.Executor;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
//Initializer = {shardMap, executor}

public class Initializer {

    Map<Integer,Partition> shardMap;
    Executor executor;
    Map<Partition,Boolean> completionMap;
    Map<Integer, ArrayBlockingQueue<Transaction>> ready_txns;
    public Initializer(Executor zk){//setter method
        this.executor = zk;
        shardMap = new HashMap<>();
        completionMap = new HashMap<>();
        ready_txns = new ConcurrentHashMap<>();
    }
    
    public Executor getExecutor() {
        return executor;
    }
    
    public void initializePartitions(int noOfPartitions, Store store){ //init function 
        int slice = store.getSize() / noOfPartitions; //splitting array into equal number of parts
        int noOfRowsAssigned =0 ;
        int no = 0;
        while(noOfRowsAssigned < store.getSize()){
            int start = noOfRowsAssigned;//initialIndex and finalIndex to the array
            int end = start + slice-1;
            addNewPartition(no,new Range(start,end));
            noOfRowsAssigned = end + 1;
            no++;
        }
    }
    
    public Map<Integer, Partition> getShardMap() {
        return shardMap;
    }

    public void printTransactions(){
        try {
            List<String> transactions = executor.getZooKeeper().getChildren("/orderer",false);
            transactions.forEach(System.out::println);
        } catch (KeeperException | InterruptedException e) {
//            e.printStackTrace();
        }

    }

    public boolean isExecutionCompleted(){
        return false;
    }



    public void addNewPartition(int no,Range range) {
        Partition partition = new Partition(no,this,range);
        try {
            byte[] input = SerializationUtils.serialize(range);
            shardMap.put(no,partition);
            completionMap.put(partition,false);
            ready_txns.put(no, new ArrayBlockingQueue<>(1000));
            executor.getZooKeeper().create("/services/active" + partition.getId(),input, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            executor.getZooKeeper().addWatch("/services/active" + partition.getId(), watchedEvent -> {
                if(watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted){
                    addNewPartition(no,range);
                    System.out.println("Node Deleted");
                }else if(watchedEvent.getType() == Watcher.Event.EventType.NodeCreated){
                    partition.start();
                }
            }, AddWatchMode.PERSISTENT);
            partition.start();
        } catch (KeeperException | InterruptedException e) {
//            e.printStackTrace();
        }
    }
//
//    public void markCompleted(int key){
//        completionMap.put(shardMap.get(key),true);
//    }

    public void purgeCompletedTransactions() {
        try {
            executor.getZooKeeper().delete("/orderer",-1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public Map<Integer, ArrayBlockingQueue<Transaction>> getReady_txns() {
        return ready_txns;
    }
}
