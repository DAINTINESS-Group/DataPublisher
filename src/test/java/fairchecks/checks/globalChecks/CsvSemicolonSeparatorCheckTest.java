package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvSemicolonSeparatorCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void semicolonSeparatorCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame1");
		
		CsvSemicolonSeparatorCheck check = new CsvSemicolonSeparatorCheck();
		boolean result = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the semicolon separator check.", result);
	}
	
	@Test
	public void semicolonSeparatorWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame2");
		
		CsvSemicolonSeparatorCheck check = new CsvSemicolonSeparatorCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("Expected wrong semicolon separator.", result);
	}
}
