package partition;

import model.Transaction;
import storage.LockManager;
import storage.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TxnProcessor {
    ArrayBlockingQueue<Transaction> consumer;
    LockManager lockManager;
    boolean isStopped;
    int partitionId;
    Range range;
    Store store;

    public TxnProcessor(int partitionId, Store store, LockManager lockManager, Range range){
        consumer  = new ArrayBlockingQueue<>(1000);
        new Thread(this::process).start(); //why new thread here ?
        this.store = store;
        this.lockManager = lockManager;
        this.partitionId = partitionId;
        isStopped = false;
        this.range = range;
        new Thread(this::executeProcessedTransaction).start();

    }

    private void process() {
        while(!isStopped){
            try {
                Transaction curr = consumer.take();
                if (!curr.isMultiPartition() && curr.getOriginatorPartition() != partitionId) {
                    continue;
                }
                System.out.println("Transaction starting " + curr.getTransactionId() + "Partition : " + partitionId) ;
                lockManager.initializeLockAcquired(curr);
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
//                if(lockManager.{
//                    System.out.println(" Direct Ready : Adding Transaction :" + curr.getTransactionId() + "lock request Made" + requestForLocksMade + "Partition Id : "  + partitionId) ;
//                    lockManager.addToReadyQueue(curr);
//                    curr.markCompleted(System.nanoTime());
//                }

            } catch (InterruptedException e) {
//                e.printStackTrace();
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
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                }
//                System.out.println("I'm Got Access " + curr.getTransactionId() + "Partition :" + partitionId);
                if (!curr.isMultiPartition() && curr.getOriginatorPartition() != partitionId) {
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
//                count.getAndIncrement();
                System.out.println("Transaction executed : " + curr.getTransactionId() + "Partition :" + partitionId);
//                if(count.get() == 100){
//                    System.out.println("Execution Completed at : " + curr.getCompletionTime());
//                }
//                if("T0000009999".equals(curr.getTransactionId())){
//                }
            }
        }
    }

    public void stop() {
        //TODO graceful stop do now.
        isStopped = true;
    }

    public void processTransaction(Transaction txn){
        try {
            consumer.put(txn);
        } catch (InterruptedException e) {
//            e.printStackTrace();
        }
    }

}
