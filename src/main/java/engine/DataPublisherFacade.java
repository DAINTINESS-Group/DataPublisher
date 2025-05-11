package engine;

import model.DatasetProfile;
import model.FairCheckResult;
import report.IChecksReportGenerator;
import report.ChecksReportGeneratorFactory;
import utils.RegistrationResponse;
import utils.ReportType;

import java.util.List;
import java.util.Map;

public class DataPublisherFacade implements IDataPublisherFacade{
	
	private DatasetRegistrationController datasetRegistrationController = new DatasetRegistrationController();
	private GlobalFairCheckService globalCheckService = new GlobalFairCheckService();
	private ColumnFairCheckService columnCheckService = new ColumnFairCheckService();
	
	@Override
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader) {
		System.out.print("Registering the dataset...");
		System.out.print("\n");
		return datasetRegistrationController.registerDataset(path, alias, hasHeader);
	}
	
	@Override
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias) throws IllegalStateException {
		System.out.print("Executing global checks on the dataset...");
		System.out.print("\n");
		DatasetProfile profile = datasetRegistrationController.getProfile(datasetAlias);

	    if (profile == null) {
	    	throw new IllegalStateException("Error: Dataset has not been registered");
	    }

	    Map<String, Boolean> results = globalCheckService.executeGlobalChecks(profile.getDataset());

	    return results;
	}
	
	@Override
    public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias) throws IllegalStateException {
		System.out.print("Executing checks on individual columns...");
		System.out.print("\n");
		DatasetProfile profile = datasetRegistrationController.getProfile(datasetAlias);
        if (profile == null) {
        	throw new IllegalStateException("Error: Dataset has not been registered");
        }
        return columnCheckService.executeColumnChecks(profile);
    }
	
	@Override
	public void generateGlobalReport(String datasetAlias, Map<String, Boolean> globalResults, String outputPath, ReportType reportType) {
		System.out.print("Generating report based on global checks...");
		System.out.print("\n");
		IChecksReportGenerator reportGenerator = new ChecksReportGeneratorFactory().createReportGenerator(reportType);
		if (reportGenerator != null) {
			reportGenerator.generateGlobalReport(datasetAlias, globalResults, outputPath);
		} else {
			throw new IllegalArgumentException("Unsupported report type: " + reportType);
		}
	}

	@Override
	public void generateColumnReport(String datasetAlias, Map<String, Map<String, List<FairCheckResult>>> columnResults, String outputPath, ReportType reportType) {
		System.out.print("Generating report based on individual - column checks...");
		System.out.print("\n");
		IChecksReportGenerator reportGenerator = new ChecksReportGeneratorFactory().createReportGenerator(reportType);
		if (reportGenerator != null) {
			reportGenerator.generateColumnReport(datasetAlias, columnResults, outputPath);
		} else {
			throw new IllegalArgumentException("Unsupported report type: " + reportType);
		}
	}

	public DatasetProfile getProfile(String alias) {
		return datasetRegistrationController.getProfile(alias);
	}

}
