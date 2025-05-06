package fairchecks.checks.globalChecks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import engine.FacadeFactory;
import engine.IDataPublisherFacade;

public class GlobalHelperClass {
	
	public static IDataPublisherFacade setUpFacade() {
		FacadeFactory facadeFactory = new FacadeFactory();
		IDataPublisherFacade facade = facadeFactory.createDataPublisherFacade();
		facade.registerDataset("src\\test\\resources\\datasets\\fruits_test.csv", "frame1", true);
		facade.registerDataset("src\\test\\resources\\datasets\\fruits_test_wrong.csv", "frame2", true);
		facade.registerDataset("src\\test\\resources\\datasets\\students_test.csv", "frame3", true);
		facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong.csv", "frame4", true);
		facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong2.csv", "frame5", true);
		facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong3.csv", "frame6", false);
		facade.registerDataset("src\\test\\resources\\datasets\\excel_test.xlsx", "frame7", true);
		facade.registerDataset("src\\test\\resources\\datasets\\excel_test_wrong.xlsx", "frame8", true);
		//facade.registerDataset("src\\test\\resources\\datasets\\noAccess_test.csv", "frame9", true);
		return facade;
	}
	
	public static Dataset<Row> getDataset(IDataPublisherFacade facade, String frameName) throws Exception {
		return facade.getProfile(frameName).getDataset();
	}

}
