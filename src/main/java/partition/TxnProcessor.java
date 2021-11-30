package partition;

import model.Transaction;
import storage.LockManager;
import storage.Store;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TxnProcessor {
    ArrayBlockingQueue<Transaction> consumer;
    LockManager lockManager;
    boolean isStopped;
    Store store;
    public TxnProcessor(Store store, LockManager lockManager){
        consumer  = new ArrayBlockingQueue<>(1000);
        new Thread(this::process).start();
        this.store = store;
        this.lockManager = lockManager;
        isStopped = false;
    }

    private void process() {
        while(!isStopped){
            try {
                Transaction curr = consumer.take();
                AtomicBoolean isBlocked = new AtomicBoolean(false);
                if(!curr.getReadSet().isEmpty()){
                    curr.getReadSet().forEach((key) -> {
                        if(!lockManager.readLock(curr,key)){
                            isBlocked.set(true);
                        }
                    });
                }
                if(!curr.getWriteSet().isEmpty()){
                    curr.getWriteSet().forEach((key) -> {
                        if(!lockManager.writeLock(curr,key)){
                            isBlocked.set(true);
                        }
                    });
                }
                if(!isBlocked.get()){
                    lockManager.addToReadyQueue(curr);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
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
