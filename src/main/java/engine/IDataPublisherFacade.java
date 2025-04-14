package engine;

import java.util.List;
import java.util.Map;

import model.DatasetProfile;
import model.FairCheckResult;
import utils.RegistrationResponse;

public interface IDataPublisherFacade {
	
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias);
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias);
	public DatasetProfile getProfile(String alias);
}
