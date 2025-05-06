package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvUnitInDedicatedColumnCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void unitInDedicatedColumnCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame1");
		
		CsvUnitInDedicatedColumnCheck check = new CsvUnitInDedicatedColumnCheck();
    	boolean validResult = check.executeCheck(dataset);
        assertTrue("Expected correct dataset to pass the unit-in-column check.", validResult);
	}
	
	@Test
	public void unitInDedicatedColumnWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame2");
		
		CsvUnitInDedicatedColumnCheck check = new CsvUnitInDedicatedColumnCheck();
    	boolean result = check.executeCheck(dataset);
    	assertFalse("Expected multiple units in a dedicated column.", result);
	}
}
