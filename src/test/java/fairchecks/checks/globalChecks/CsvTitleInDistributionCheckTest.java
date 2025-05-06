package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvTitleInDistributionCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void titleInDistributionCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		CsvTitleInDistributionCheck check1 = new CsvTitleInDistributionCheck();
		boolean result1 = check1.executeCheck(dataset);
		assertTrue("Expected no title inside this dataset.", result1);
	}
	
	@Test
	public void titleInDistributionWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame6");
		
		CsvTitleInDistributionCheck check2 = new CsvTitleInDistributionCheck();
		boolean result2 = check2.executeCheck(dataset);
		assertFalse("Expected title inside this dataset.", result2);
	}
}
