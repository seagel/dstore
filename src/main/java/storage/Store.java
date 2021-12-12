package storage;

import java.util.ArrayList;
import java.util.Collections;

public class Store { //Array 
    ArrayList<Integer> store;
    public Store(){
        store = new ArrayList<>(Collections.nCopies(100, 0));
    }
    public int getSize(){
        return store.size();
    }
    public void write(int key,int value){
        store.set(key,value);
    }
    public int read(int key){
        if(key > store.size())
            return -1;
        return store.get(key);
    }
}
