package Tests;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import constants.ConfigurableConstants;
import model.Transaction;

public class GenerateTransactions {

	
	/**
	 * 0 - Read-only mode
	 * 1 - Read-Write mode
	 * @param length
	 * @param contention
	 * @param mode
	 * @return
	 */
	public Transaction genTrans(int length, float percentageRead, float contention, int mode)
	{
		Set<Integer> readset = new HashSet<Integer>();
		Set<Integer> writeset = new HashSet<Integer>();
		
		Random random = new Random();
		int lengthRead = (int) (percentageRead*length/100);
		int lengthReadWrite = (int)((1-percentageRead)*length/100);
		for(int i=0; i< lengthRead; i++)
		{
			int read = (int) ((random.nextInt()) % (100 + (ConfigurableConstants.DATABASE_SIZE-100)*(1-contention)));
			readset.add(read);
		}
		if(mode == 1)
			for(int i=0; i<lengthReadWrite; i++)
			{
				int write = (int) ((random.nextInt()) % (100 + (ConfigurableConstants.DATABASE_SIZE-100)*(1-contention)));
				writeset.add(write);
			}
		return new Transaction(readset,writeset,true);
	}

	public List<Transaction> listoftxns(int length, float percentageRead, float contention, int mode)
	{
		List<Transaction> transactionList = new ArrayList<Transaction>();
		for(int i=0; i<=ConfigurableConstants.PROCESSING_QUEUE_SIZE; i++)
		{
			transactionList.add(genTrans(length,percentageRead,contention,mode));
		}
		return transactionList;
	}


}
