package partition;

import model.Transaction;
import storage.LockManager;
import storage.Store;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
                AtomicBoolean isBlocked = new AtomicBoolean(false);
                if(!curr.getReadSet().isEmpty()){
                    for(int key :curr.getReadSet()){
                        if(doesItBelongToMe(key) && !lockManager.readLock(curr,key)){
                            isBlocked.set(true);
                        }
                    }
                }
                if(!curr.getWriteSet().isEmpty()){
                    for(int key : curr.getWriteSet()){
                        if(doesItBelongToMe(key) && !lockManager.writeLock(curr,key)){
                            isBlocked.set(true);
                        }
                    }
                }
                if(!isBlocked.get()){
                    lockManager.addToReadyQueue(curr);
                }else{
                    System.out.println("I'm Blocked " + curr);
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
            while (!lockManager.getReady_txns().isEmpty()) {
                Transaction curr = null;
                try {
                    curr = lockManager.getReady_txns().take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("I'm Got Access " + curr);
                if (!curr.isMultiPartition() && curr.getOriginatorPartition() != partitionId) {
                    continue;
                }
                for (int key : curr.getWriteSet()) {
                    store.write(key, store.read(key) + 1);
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (!curr.getReadSet().isEmpty()) {
                    for (int key : curr.getReadSet()) {
                        if(doesItBelongToMe(key))
                            lockManager.release(curr, key);
                    }
                }
                if (!curr.getWriteSet().isEmpty()) {
                    for (int key : curr.getWriteSet()) {
                        if(doesItBelongToMe(key))
                            lockManager.release(curr, key);
                    }
                }
                System.out.println("Transaction executed successfully" + curr);
            }
        }
    }

    public void stop() {
        //TODO graceful stop do now.
        isStopped = true;
    }

    public void processTransaction(Transaction txn){
        consumer.add(txn);
    }

}
