package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class UriDetailsRetrievalCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void uriDetailsRetrievalCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		UriDetailsRetrievalCheck check = new UriDetailsRetrievalCheck();
		boolean validResult = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the URI details retrieved check.", validResult);
	}
	
	@Test
	public void uriDetailsRetrievalWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame5");
		
		UriDetailsRetrievalCheck check = new UriDetailsRetrievalCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("The dataset hasn't a label column for the URI column", result);
	}
}
