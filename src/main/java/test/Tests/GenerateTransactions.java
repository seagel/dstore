package test.Tests;
import java.util.*;

import constants.ConfigurableConstants;
import model.Transaction;
import partition.Range;

public class GenerateTransactions {

	ArrayList<Range> ranges;
	public GenerateTransactions(ArrayList<Range> ranges){
		this.ranges = ranges;
	}
	
	/**
	 * 0 - Read-only mode
	 * 1 - Read-Write mode
	 */
	public  Transaction genTrans(int length, float percentageRead, float contention,float mHRate) {
		SortedSet<Integer> readSet = new TreeSet<>();
		SortedSet<Integer> writeSet = new TreeSet<>();
		
		Random random = new Random();
		int lengthRead = (int) (percentageRead*length);
		int lengthReadWrite = length - lengthRead;
		int mhRange = (int)(ranges.size() * mHRate);
		if(contention == 1){
			mhRange = Math.min(mhRange,3);
		}
		int count =0,i=0;
		while(i< lengthRead) {
			int read = random.nextInt(ranges.get(count).getEnd()- ranges.get(count).getStart()) + ranges.get(count).getStart();
			if(!readSet.contains(read)){
				readSet.add(read);
				i=i+1;
				count=count+1;
				count=count%mhRange;
			}

		}
		// write multi home transactions logic
		i=0;
		while(i<lengthReadWrite) {
			int write = random.nextInt(ranges.get(count).getEnd()- ranges.get(count).getStart()) + ranges.get(count).getStart();
			if(!readSet.contains(write) && !writeSet.contains(write)) {
				writeSet.add(write);
				i=i+1;
				count = count + 1;
				count = count % mhRange;
			}
		}
		return new Transaction(readSet,writeSet,true);
	}



	public List<Transaction> listoftxns(int length, float percentageRead, float contention,float mHRate) {
		List<Transaction> transactionList = new ArrayList<>();
		Transaction t = genTrans(length,percentageRead,contention,mHRate);
		for(int i=0; i<ConfigurableConstants.TRANSACTION_LENGTH; i++) {
			transactionList.add(t);
		}
		return transactionList;
	}


}
