package fairchecks.api;

import org.apache.spark.sql.types.DataType;

public interface IGenericApplicableCheck extends IGenericCheck{
	public boolean isApplicable(DataType columnType);
}
