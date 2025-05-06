package fairchecks.checks.columnChecks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import engine.FacadeFactory;
import engine.IDataPublisherFacade;

public class ColumnHelperClass {
	
	public static IDataPublisherFacade setUpFacade() {
		FacadeFactory facadeFactory = new FacadeFactory();
		IDataPublisherFacade facade = facadeFactory.createDataPublisherFacade();
		facade.registerDataset("src\\test\\resources\\datasets\\fruits_test.csv", "frame1", true);
		facade.registerDataset("src\\test\\resources\\datasets\\fruits_test_wrong.csv", "frame2", true);
		return facade;
	}
	
	public static Dataset<Row> getDataset(IDataPublisherFacade facade, String frameName) throws Exception {
		return facade.getProfile(frameName).getDataset();
	}
}
