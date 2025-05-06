package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class SufficientDataCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void sufficientDataCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame1");
		
		SufficientDataCheck check = new SufficientDataCheck();
		boolean validResult = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the sufficient data check.", validResult);
	}
	
	@Test
	public void sufficientDataWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame2");
		
		SufficientDataCheck check = new SufficientDataCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("The dataset has not enough data", result);
	}
}
