package fairchecks.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface IGenericCheck {
		public String getCheckId();
		public String getCheckDescription();
	    public boolean executeCheck(Dataset<Row> dataset);
}
