package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class DataAccuracyCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void dataAccuracyTest() throws Exception {
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame1");
		
		String[] columnsToCheck = new String[] { "weight (g)", "quantity" };
		
		for (String column : columnsToCheck) {
			DataAccuracyCheck check = new DataAccuracyCheck(column);
			
			DataType columnType = dataset.schema().apply(column).dataType();
			check.isApplicable(columnType);
			
			boolean result = check.executeCheck(dataset);
			
			if (column.equals("weight (g)")) {
				assertTrue("Expected column '" + column + "' to pass data accuracy check.", result);
			} else {
				assertFalse("Expected column '" + column + "' to fail data accuracy check.", result);
			}
		}
	}
}
