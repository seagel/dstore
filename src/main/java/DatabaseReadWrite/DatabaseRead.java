package DatabaseReadWrite;

import java.util.*;
import java.util.stream.IntStream;

import model.Transaction;

import static java.util.stream.Collectors.*;

import partition.Partition;
import partition.Range;

public class DatabaseRead {
	
	public void performRead(Transaction t, Partition p)
	{
		Range r = p.getRange();
		Set<Integer> readSet = t.getReadSet();
		Set<Integer> writeSet = t.getWriteSet();
		//readSet.forEach(Integer s, filter readSets);
		
	}
	/**
	 * 1. Filter the read sets belonging to the respective partition {partition and read_set should be the input parameters}
	 * The above call has to be multi-threaded, use as much functional programming as possible
	 * The above call will be initiated by individual threads [async & multi thread enabled]
	 * 2. Read the value of the array at this index - just a direct reference 
	 * Note that the store allows shared locks [reading requires no locks], needs the array index wise lock [need to check?]
	 * 3. Before calling this function use a lookup table which is used as a reference by each thread - partition is the key , and the last read thread index will be the index
	 * 
	 */

}
