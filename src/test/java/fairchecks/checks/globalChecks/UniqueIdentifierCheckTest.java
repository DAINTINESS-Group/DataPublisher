package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class UniqueIdentifierCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void uniqueIdentifierCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		UniqueIdentifierCheck check = new UniqueIdentifierCheck();
		boolean validResult = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the unique URI check.", validResult);
	}
	
	@Test
	public void uniqueIdentifierWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame4");
		
		UniqueIdentifierCheck check = new UniqueIdentifierCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("The dataset hasn't valid URI column", result);
	}
}
