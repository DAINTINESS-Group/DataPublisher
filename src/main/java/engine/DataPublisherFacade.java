package engine;

import model.DatasetProfile;
import model.FairCheckResult;
import report.FairCheckReportGenerator;
import utils.RegistrationResponse;

import java.util.List;
import java.util.Map;

public class DataPublisherFacade implements IDataPublisherFacade{
	
	private DatasetRegistrationController datasetRegistrationController = new DatasetRegistrationController();
	private GlobalFairCheckService fairCheckService = new GlobalFairCheckService();
	private ColumnFairCheckService columnCheckService = new ColumnFairCheckService();
	
	@Override
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader) {
		return datasetRegistrationController.registerDataset(path, alias, hasHeader);
	}
	
	@Override
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias) throws IllegalStateException {
	    DatasetProfile profile = datasetRegistrationController.getProfile(datasetAlias);

	    if (profile == null) {
	    	throw new IllegalStateException("Error: Dataset has not been registered");
	    }

	    Map<String, Boolean> results = fairCheckService.executeGlobalChecks(profile.getDataset());

	    return results;
	}
	
	@Override
    public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias) throws IllegalStateException {
        DatasetProfile profile = datasetRegistrationController.getProfile(datasetAlias);
        if (profile == null) {
        	throw new IllegalStateException("Error: Dataset has not been registered");
        }
        return columnCheckService.executeColumnChecks(profile);
    }
	
	@Override
	public void generateGlobalReport(String datasetAlias, Map<String, Boolean> globalResults, String outputPath) {
	    FairCheckReportGenerator.generateGlobalReport(datasetAlias, globalResults, outputPath);
	}

	@Override
	public void generateColumnReport(String datasetAlias, Map<String, Map<String, List<FairCheckResult>>> columnResults, String outputPath) {
	    FairCheckReportGenerator.generateColumnReport(datasetAlias, columnResults, outputPath);
	}

	
	public DatasetProfile getProfile(String alias) {
		return datasetRegistrationController.getProfile(alias);
	}

}
