package engine;

import java.util.Map;

import utils.RegistrationResponse;

public interface IDataPublisherFacade {
	
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
	public RegistrationResponse registerDataset(String databaseUsername, String databasePassword, String databaseType, 
            String url, String tableName, String alias);
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias);
	Map<String, Map<String, Boolean>> executeColumnChecks(String datasetAlias);
}
