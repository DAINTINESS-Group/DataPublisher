package app;

import java.util.List;
import java.util.Map;

import engine.FacadeFactory;
import engine.IDataPublisherFacade;
import model.FairCheckResult;
import report.FairCheckReportGenerator;

public class Main {
    public static void main(String[] args) {
    	FacadeFactory factory = new FacadeFactory(); 
        IDataPublisherFacade facade = factory.createDataPublisherFacade();

        facade.registerDataset("src/test/resources/datasets/countries.csv", "myDataset", true);
        
        Map<String, Boolean> globalResults = facade.executeGlobalChecks("myDataset");
        Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks("myDataset");
        
        String outputPath = "src/main/resources/reports/FAIR_Report.txt";

        FairCheckReportGenerator.generateGlobalReport("myDataset", globalResults, outputPath);
        FairCheckReportGenerator.generateColumnReport("myDataset", columnResults, outputPath);
    }
}
