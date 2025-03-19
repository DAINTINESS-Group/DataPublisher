package engine;

import model.DatasetProfile;
import utils.RegistrationResponse;

import java.util.LinkedHashMap;
import java.util.Map;

public class DataPublisherFacade implements IDataPublisherFacade{
	
	private DatasetRegistrationController datasetController = new DatasetRegistrationController();
	private GlobalFairCheckService fairCheckService = new GlobalFairCheckService();
	private ColumnFairCheckService columnCheckService = new ColumnFairCheckService();
	
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader) {
		return datasetController.registerDataset(path, alias, hasHeader);
	}
	
	public RegistrationResponse registerDataset(String username, String password, String databaseType,
								String url, String tableName, String alias) {
		return datasetController.registerDataset(username, password, databaseType, url, tableName, alias);
	}
	
	@Override
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias) {
	    DatasetProfile profile = datasetController.getProfile(datasetAlias);

	    if (profile == null) {
	        System.out.println("Error: Dataset not found.");
	        return new LinkedHashMap<>();
	    }

	    Map<String, Boolean> results = fairCheckService.executeGlobalChecks(profile.getDataset());

	    System.out.println("FAIR Check Results for: " + datasetAlias);
	    results.forEach((question, passed) ->
	        System.out.println(question + ": " + (passed ? "Passed" : "Failed"))
	    );
	    return results;
	}
	
	@Override
    public Map<String, Map<String, Boolean>> executeColumnChecks(String datasetAlias) {
        DatasetProfile profile = datasetController.getProfile(datasetAlias);
        if (profile == null) {
            System.out.println("Dataset not found: " + datasetAlias);
            return null;
        }
        return columnCheckService.executeColumnChecks(profile);
    }
	
	private DatasetProfile getProfile(String alias) {
		return datasetController.getProfile(alias);
	}

}
