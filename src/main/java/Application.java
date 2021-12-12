import constants.ConfigurableConstants;
import partition.Initializer;
import partition.Range;
import partition.TxnProcessor;
import storage.LockManager;
import storage.Store;
import test.Tests.LoadTest;
import zookeeper.Executor;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
//package Zookeeper.executor

public class Application {
    public static void main(String[] args) {
        Executor connection = new Executor("localhost");
        Initializer initializer = new Initializer(connection);
        Store store = new Store();
        initializer.initializePartitions(ConfigurableConstants.NUMBER_OF_PARTITIONS, store);
        LockManager lockManager = new LockManager(initializer.getReady_txns());
        initializer.getShardMap().forEach((key, value) -> {
            TxnProcessor processor = new TxnProcessor(key, store, lockManager, value.getRange());
            value.setTxnProcessor(processor);
        });

//        // read only transactions
//        System.out.println("Read Only Transactions - Low Contention : " );
//        long startTime = System.nanoTime();
//
//        initializer.getShardMap().forEach((key, value) ->
//            value.pushToProduceQueue(LoadTest.readOnlyLowContention()));
//
//        try {
//            TimeUnit.MILLISECONDS.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        while(!lockManager.isCompleted()){
//
//        }
//        System.out.println("Execution Time : " + (System.nanoTime() - startTime)/10000);


//        System.out.println("Read Only Transactions - High Contention : " );
//        long startTime = System.nanoTime();
//
//        initializer.getShardMap().forEach((key, value) ->
//                value.pushToProduceQueue(LoadTest.readOnlyHighContention()));
//
//        long endTime = System.nanoTime();
//        System.out.println("Execution Time : " + (endTime - startTime)/100000);
        ArrayList<Range> ranges = new ArrayList<>();
        initializer.getShardMap().forEach((key,value)-> ranges.add(value.getRange()));
        System.out.println("Read-Write Only Transactions - Low Contention : " );
        long startTime = System.nanoTime();
        LoadTest loadTest = new LoadTest(ranges);
        System.out.println("Start Time :  " +  startTime);
        initializer.getShardMap().forEach((key, value) ->
                value.pushToProduceQueue(loadTest.test1()));
        try {
            TimeUnit.MILLISECONDS.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        while(!lockManager.isCompleted()){
//
//        }
//        System.out.println("Time Taken for Execution : " + (System.nanoTime() - startTime )/10000);

}
}
