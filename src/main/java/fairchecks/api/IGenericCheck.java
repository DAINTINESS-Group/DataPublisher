package fairchecks.api;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface IGenericCheck {
		public String getCheckId();
		public String getCheckDescription();
	    public boolean executeCheck(Dataset<Row> dataset);
	    public default List<String> getInvalidRows() { return null; }
}
