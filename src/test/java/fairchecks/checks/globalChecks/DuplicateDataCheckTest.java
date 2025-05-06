package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class DuplicateDataCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void duplicateDataCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame1");
		
		DuplicateDataCheck check = new DuplicateDataCheck();
		boolean validResult = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the duplicate data check.", validResult);
	}
	
	@Test
	public void duplicateDataWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame2");
		
		DuplicateDataCheck check = new DuplicateDataCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("The dataset has a duplicate row", result);
	}
}
