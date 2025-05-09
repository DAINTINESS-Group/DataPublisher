package app;

import java.util.List;
import java.util.Map;

import engine.FacadeFactory;
import engine.IDataPublisherFacade;
import model.FairCheckResult;
import utils.ReportType;

public class Main {
    public static void main(String[] args) {
    	FacadeFactory factory = new FacadeFactory(); 
        IDataPublisherFacade facade = factory.createDataPublisherFacade();
        
        facade.registerDataset("src/test/resources/datasets/countries.csv", "myDataset", true);
        
        Map<String, Boolean> globalResults = facade.executeGlobalChecks("myDataset");
        Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks("myDataset");
        
        String outputFolder = "src/test/resources/reports/";
        ReportType reportTypeTXT = ReportType.TEXT;
        
        facade.generateGlobalReport("myDataset", globalResults, outputFolder + "FAIR_Report.txt", reportTypeTXT);
        facade.generateColumnReport("myDataset", columnResults, outputFolder + "FAIR_Report.txt", reportTypeTXT);
        
        ReportType reportTypeMD = ReportType.MARKDOWN;
        facade.generateGlobalReport("myDataset", globalResults, outputFolder + "FAIR_Report.md", reportTypeMD);
        facade.generateColumnReport("myDataset", columnResults, outputFolder + "FAIR_Report.md", reportTypeMD);
    }
}
