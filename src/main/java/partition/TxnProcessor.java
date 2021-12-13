package partition;

import constants.ConfigurableConstants;
import model.Transaction;
import storage.LockManager;
import storage.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.max;


public class TxnProcessor {
    ArrayBlockingQueue<Transaction> consumer;
    LockManager lockManager;
    boolean isStopped,stopped ;
    int partitionId;
    Range range;
    Store store;
    Map<String,Integer> countExecution = new ConcurrentHashMap<>();
    long endTimestamp;
    long startTimestamp;



    long totalTransactions;

    public TxnProcessor(int partitionId, Store store, LockManager lockManager, Range range){
        consumer  = new ArrayBlockingQueue<>(1000);
        new Thread(this::process).start(); //why new thread here ?
        this.store = store;
        this.lockManager = lockManager;
        this.partitionId = partitionId;
        isStopped = stopped =  false;
        this.range = range;
        endTimestamp = 0;
        new Thread(this::executeProcessedTransaction).start();

    }

    private void process() {
        while(!isStopped){
            try {
                Transaction curr = consumer.take();
                if (!curr.isMultiPartition() && curr.getOriginatorPartition() != partitionId) {
                    countExecution.putIfAbsent(curr.getTransactionId(),0);
                    countExecution.merge(curr.getTransactionId(),1,Integer::sum);
                    continue;
                }
//                System.out.println("Transaction starting " + curr.getTransactionId() + "Partition : " + partitionId) ;
                if(!curr.getReadSet().isEmpty()){
                    for(int key :curr.getReadSet()){
                        if(doesItBelongToMe(key) ){
                            lockManager.readLock(curr,key,partitionId);
                        }
                    }
                }
                if(!curr.getWriteSet().isEmpty()){
                    for(int key : curr.getWriteSet()){
                        if(doesItBelongToMe(key) ){
                            lockManager.writeLock(curr,key,partitionId);
                        }
                    }
                }


            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
    private boolean doesItBelongToMe(int key){
        return range.start<=key&&range.end>=key;
    }
    public void executeProcessedTransaction()  {
        while(!isStopped) {
            while (!lockManager.getReady_txns(partitionId).isEmpty()) {
                Transaction curr = null;
                try {
                    curr = lockManager.getReady_txns(partitionId).take();
                    if(countExecution.containsKey(curr.getTransactionId()))
                        continue;
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                }
//                System.out.println("I'm Got Access " + curr.getTransactionId() + "Partition :" + partitionId);
                if (!curr.isMultiPartition() && curr.getOriginatorPartition() != partitionId) {
                    countExecution.putIfAbsent(curr.getTransactionId(),0);
                    countExecution.merge(curr.getTransactionId(),1,Integer::sum);
                    continue;
                }
                for (int key : curr.getWriteSet()) {
                    if(doesItBelongToMe(key))
                        store.write(key, store.read(key) + 1);
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                }
                if (!curr.getReadSet().isEmpty()) {
                    for (int key : curr.getReadSet()) {
                        if(doesItBelongToMe(key) ){
                            lockManager.release(curr, key,partitionId);
                        }
                    }
                }
                if (!curr.getWriteSet().isEmpty()) {
                    for (int key : curr.getWriteSet()) {
                        if(doesItBelongToMe(key))
                            lockManager.release(curr, key,partitionId);
                    }
                }
                curr.markCompleted(System.nanoTime());
                endTimestamp = Math.max(endTimestamp,curr.getCompletionTime());
                lockManager.removeLockAcquired(curr.getTransactionId());
                countExecution.putIfAbsent(curr.getTransactionId(),0);
                countExecution.merge(curr.getTransactionId(),1,Integer::sum);
                int txnExecuted = countExecution.values().stream().reduce(0, Integer::sum);
                System.out.println("Transaction executed : " + txnExecuted + " Timestamp : " + curr.getCompletionTime());

                if( txnExecuted == ConfigurableConstants.NUMBER_OF_PARTITIONS *ConfigurableConstants.TRANSACTION_LENGTH){
                    System.out.println("Execution Completed : " + (System.nanoTime()  - startTimestamp)/totalTransactions);
                }
            }
        }
    }

//    private boolean isComplete(int value) {
//        if(value%100 == 0){
//            System.out.println("Executed transactions :" + value + " Partition :" + partitionId);
//        }
//        return value == totalTransactions;
//    }
//    public boolean isStopped(){
//        return isStopped;
//    }

    public void stop() {
        //TODO graceful stop do now.
        isStopped = true;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public void setTotalTransactions(long totalTransactions) {
        this.totalTransactions = totalTransactions;
    }


    public void processTransaction(Transaction txn){
        try {
            consumer.put(txn);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
