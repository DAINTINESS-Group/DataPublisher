package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class ControlledVocabularyCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void controlledVocabularyCorrectTest() throws Exception {
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame1");
		
		String[] columnsToCheck = new String[] { "currency" };
		
		for (String column : columnsToCheck) {
			ControlledVocabularyCheck check = new ControlledVocabularyCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertTrue("Expected column '" + column + "' to pass controlled vocabulary check.", result);
		}
	}
	
	@Test
	public void controlledVocabularyWrongTest() throws Exception {
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = new String[] { "currency" };
		
		for (String column : columnsToCheck) {
			ControlledVocabularyCheck check = new ControlledVocabularyCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertFalse("Expected column '" + column + "' to fail controlled vocabulary check.", result);
		}
	}
}
