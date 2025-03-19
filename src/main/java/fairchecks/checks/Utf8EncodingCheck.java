package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Utf8EncodingCheck implements IInteroperabilityCheck{
	
	@Override
	public String getCheckId() {
		return "IEU3.1";
	}
	
	@Override
	public String getCheckDescription() {
		return "The encoding of choice on the web is UTF-8, and must be explicitly enabled on ‘Save As’.";
	}
	
	@Override
	public boolean executeCheck(Dataset<Row> dataset) {
		try {
			String encoding = dataset.sparkSession().conf().get("spark.sql.csv.encoding", "UTF-8");
			return encoding.equalsIgnoreCase("UTF-8");
		} catch(Exception e) {
			System.err.println("Error execution UTF-8 Encoding Check: " + e.getMessage());
			return false;
		}
	}

}
