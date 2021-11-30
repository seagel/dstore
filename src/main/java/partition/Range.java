package partition;

import java.io.Serializable;

public class Range implements Serializable {

    int start,end;
    public Range(int start,int end){
        this.start = start;
        this.end =end;
    }
    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }
}
