package storage;

import model.Transaction;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
enum LockMode {
    SHARED,
    EXCLUSIVE,
    UNLOCKED
}
class LockRequest{
    Transaction txn;
    LockMode mode;
    public LockRequest(Transaction txn,LockMode mode){
        this.txn = txn;
        this.mode =mode;
    }
}
public class LockManager {
    private ConcurrentHashMap<Integer, Deque<LockRequest>> lockTable ;
    private Map<Transaction,Integer> txn_waits;
    ArrayBlockingQueue<Transaction> ready_txns;

    public LockManager(){
        lockTable = new ConcurrentHashMap<>();
        txn_waits = new ConcurrentHashMap<>();
        ready_txns = new ArrayBlockingQueue<>(10000);
    }
    synchronized public boolean writeLock(Transaction txn,int key){
        boolean isLockGranted = false;
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty()){
            isLockGranted = true;
        }else{
            txn_waits.merge(txn, 1, Integer::sum);
        }

        LockRequest lockRequest = new LockRequest(txn,LockMode.EXCLUSIVE);
        Deque<LockRequest> temp = new ConcurrentLinkedDeque<>();

        if(lockRequests == null){
            temp.addLast(lockRequest);
            lockTable.put(key,temp);
        }else{
            lockRequests.add(lockRequest);
        }
//        System.out.println("Write lock for transaction : " + txn.toString() + " Granted : " + isLockGranted + " key :" + key);

        return isLockGranted;
    }

    synchronized public boolean readLock(Transaction txn,int key){
        boolean isLockGranted = false;
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty()){
            isLockGranted = true;
        }
        synchronized (this){
            LockRequest lockRequest = new LockRequest(txn, LockMode.SHARED);
            Deque<LockRequest> temp = new ConcurrentLinkedDeque<>();
            if (lockRequests == null) {
                temp.addLast(lockRequest);
                lockTable.put(key, temp);
            } else {
                lockRequests.add(lockRequest);
            }
        }
        lockRequests = lockTable.get(key);
        Iterator<LockRequest> it = lockRequests.iterator();
        boolean isExclusiveLock = it.next().mode == LockMode.EXCLUSIVE;
        if(!isLockGranted && !isExclusiveLock){
            isLockGranted = true;
            while(it.hasNext()){
                if(it.next().mode != LockMode.SHARED){
                    isLockGranted = false;
                    break;
                }
            }
        }
        if(!isLockGranted){
            txn_waits.merge(txn, 1, Integer::sum);
        }
        System.out.println("Read lock for transaction : " + txn.toString() + " Granted : " + isLockGranted + " key :" + key);
        return isLockGranted;
    }

    synchronized public void release(Transaction txn,int key){
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests.isEmpty())
            return;
        Iterator<LockRequest> it = lockRequests.iterator();
        while(it.hasNext()){
            LockRequest curr = it.next();
            if(curr.txn == txn){
                lockRequests.remove(curr);
                break;
            }
        }
        if(lockRequests.isEmpty())
            return;

        it = lockRequests.iterator();
        boolean isShared = lockRequests.getFirst().mode == LockMode.SHARED;

        if(!isShared && lockRequests.getFirst().txn != txn){
            txn_waits.merge(txn,-1,Integer::sum);
            if(txn_waits.get(lockRequests.getFirst().txn) == 0)
                ready_txns.add(lockRequests.getFirst().txn);
        }else{
            while(it.hasNext()){
                LockRequest curr = it.next();
                if(isShared && curr.mode == LockMode.EXCLUSIVE)
                    break;
                else if(txn != curr.txn){
                    txn_waits.merge(txn,-1,Integer::sum);
                    if(txn_waits.get(curr.txn) == 0)
                        ready_txns.add(curr.txn);
                }
            }
        }
//        System.out.println("Released lock for transaction : " + txn.toString() + " Key :"  + key);

    }

    synchronized public LockMode status(int key, List<Transaction> owners){
        Deque<LockRequest> lockRequests = lockTable.get(key);
        owners.clear();
        for (LockRequest curr : lockRequests) {
            if (curr.mode == LockMode.SHARED) {
                owners.add(curr.txn);
            } else if (curr.mode == LockMode.EXCLUSIVE && owners.isEmpty()) {
                owners.add(curr.txn);
                return LockMode.EXCLUSIVE;
            } else {
                return LockMode.SHARED;
            }
        }
        return owners.isEmpty() ? LockMode.UNLOCKED : LockMode.SHARED;
    }

    public void addToReadyQueue(Transaction txn){
        ready_txns.add(txn);
    }

    public ArrayBlockingQueue<Transaction> getReady_txns() {
        return ready_txns;
    }
}