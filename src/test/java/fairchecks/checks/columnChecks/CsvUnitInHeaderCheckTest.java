package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvUnitInHeaderCheckTest {
	
private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void unitInHeaderCorrectTest() throws Exception {
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame1");
		
		String[] columnsToCheck = new String[] { "weight (g)" };
		
		for (String column : columnsToCheck) {
			CsvUnitInHeaderCheck check = new CsvUnitInHeaderCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertTrue("Expected column '" + column + "' to pass unit in header check.", result);
		}
	}
	
	@Test
	public void unitInHeaderWrongTest() throws Exception {
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = new String[] { "weight" };
		
		for (String column : columnsToCheck) {
			CsvUnitInHeaderCheck check = new CsvUnitInHeaderCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertFalse("Expected column '" + column + "'to fail unit in header check.", result);
		}
	}
}
