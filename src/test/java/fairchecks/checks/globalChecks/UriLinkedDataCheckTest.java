package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class UriLinkedDataCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void uriLinkedDataCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		UriLinkedDataCheck check = new UriLinkedDataCheck();
		boolean validResult = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the URI Linked Data test.", validResult);
	}
	
	@Test
	public void uriLinkedDataWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame5");
		
		UriLinkedDataCheck check = new UriLinkedDataCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("The dataset can't be published as Linked Data", result);
	}
}
