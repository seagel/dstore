package Tests;
@RunWith(TestRunner.class)
public class Test1 {

    @Test
    public void test1()
    {
        List<Transaction> l1 = listoftxns(30,1);
        Partition.sendToProcessingQueue(l1);
    }
	
	

}
