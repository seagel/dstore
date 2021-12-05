package Tests;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import constants.ConfigurableConstants;
import model.Transaction;

public class GenerateTransactions {

	
	
	public Transaction genTrans(int length, int contention)
	{
		Set<Integer> readset = new HashSet<Integer>();
		Set<Integer> writeset = new HashSet<Integer>();
		
		Random random = new Random();
		if(contention == 0)
		{
			for(int i=0; i<length; i++){
				int read = (random.nextInt()) % ConfigurableConstants.DATABASE_SIZE;
				readset.add(read);
			}

			for(int i=0; i<=length; i++){
				int write = (random.nextInt()) % ConfigurableConstants.DATABASE_SIZE;
				writeset.add(write);
			}
			

		}
		else if(contention == 1)
		{
			for(int i =0; i<length; i++){
				int read = (random.nextInt()) % 100;
				readset.add(read);
			}
			for(int i=0; i<length; i++){
				int write = (random.nextInt()) % 100;
				writeset.add(write);
			}
		}
		return new Transaction(readset,writeset,true);
	}

	public List<Transaction> listoftxns(int length, int contention)
	{
		List<Transaction> transactionList = new ArrayList<Transaction>();
		for(int i=0; i<=ConfigurableConstants.PROCESSING_QUEUE_SIZE; i++)
		{
			transactionList.add(genTrans(length,contention));
		}
		return transactionList;
	}


}
