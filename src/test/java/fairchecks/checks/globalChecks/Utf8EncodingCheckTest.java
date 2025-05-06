package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class Utf8EncodingCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void utf8EncodingCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		Utf8EncodingCheck check = new Utf8EncodingCheck();
		boolean validResult = check.executeCheck(dataset);
		assertTrue("Expected correct dataset to pass the UTF-8 encoding check.", validResult);
	}
	
	@Test
	public void utf8EncodingWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame4");
		
		Utf8EncodingCheck check = new Utf8EncodingCheck();
		boolean result = check.executeCheck(dataset);
		assertFalse("The dataset hasn't UTF-8 encoding", result);
	}
}
