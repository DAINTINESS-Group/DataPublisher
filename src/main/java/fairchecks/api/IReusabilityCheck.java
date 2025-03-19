package fairchecks.api;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface IReusabilityCheck {
	String getCheckId();
	String getCheckDescription();
    boolean executeCheck(Dataset<Row> dataset);
    default List<String> getInvalidRows() { return null; }

}
