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
        
        ReportType reportTypeMD = ReportType.MARKDOWN;
        facade.generateGlobalReport("myDataset", globalResults, outputFolder + "FAIR_Report.md", reportTypeMD);
        facade.generateColumnReport("myDataset", columnResults, outputFolder + "FAIR_Report.md", reportTypeMD);
        
        Map<String, Boolean> globalResultsForIEU16 = facade.executeGlobalChecks("myDataset", "IEU16");
        ReportType reportTypeTXT = ReportType.TEXT;
        
        facade.generateGlobalReport("myDataset", globalResultsForIEU16, outputFolder + "IEU16_Results_Report.txt", reportTypeTXT);
        
        Map<String, Map<String, List<FairCheckResult>>> columnResultsForFEU2 = facade.executeColumnChecks("myDataset", "all", "FEU2");
        facade.generateColumnReport("myDataset", columnResultsForFEU2, outputFolder + "FEU2_Results_Report.md", reportTypeMD);
        
        Map<String, Map<String, List<FairCheckResult>>> columnCapitalResults = facade.executeColumnChecks("myDataset", "Capital", "all");
        facade.generateColumnReport("myDataset", columnCapitalResults, outputFolder + "Capital_Column_Results_Report.md", reportTypeMD);
        
        Map<String, Map<String, List<FairCheckResult>>> columnCapitalResultsForFEU2 = facade.executeColumnChecks("myDataset", "Capital", "FEU2");
        facade.generateColumnReport("myDataset", columnCapitalResultsForFEU2, outputFolder + "Capital_Column_FEU2_Results_Report.txt", reportTypeTXT);
    }
}
