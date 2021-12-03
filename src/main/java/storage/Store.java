package storage;

import java.util.ArrayList;
import java.util.Collections;

public class Store { //Array 
    ArrayList<Integer> store;
    public Store(){
        store = new ArrayList<>(Collections.nCopies(10000, -1));
    }
    public int getSize(){
        return store.size();
    }
}
