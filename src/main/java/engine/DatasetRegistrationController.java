package engine;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import config.SparkConfig;
import model.DatasetProfile;
import utils.RegistrationResponse;

/**
 * Responsible for registering datasets (from CSV files) into the system and storing them as {@link DatasetProfile} instances.
 * <p>
 * This controller manages Spark-based dataset loading, ensures unique aliases,
 * and exposes methods to retrieve or list registered dataset profiles.
 * A <code>RegistrationResponse</code> is returned to represent if the registration
 * was succesful.
 * </p>
 * 
 * @see DatasetProfile
 * @see RegistrationResponse
 */
public class DatasetRegistrationController {
	
	private ArrayList<DatasetProfile> datasetProfiles = new ArrayList<DatasetProfile>();
	private SparkSession spark;
	
	public DatasetRegistrationController() {
		spark = new SparkConfig().getSparkSession();
	}
	
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader) {
		try 
		{
			if(existsProfileWithAlias(alias)) return RegistrationResponse.ALIAS_EXISTS;

			Dataset<Row> df = spark.read().option("header", hasHeader)
					.option("inferSchema", true)
					.csv(path)
					.withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));

			df.sparkSession().conf().set("spark.sql.csv.filepath", path);

			DatasetProfile profile = new DatasetProfile(alias, df, path, hasHeader);
			datasetProfiles.add(profile);
			
			return RegistrationResponse.SUCCESS;
		}
		catch(Exception e) 
		{
			return RegistrationResponse.FAILURE;
		}
	}
	
	private boolean existsProfileWithAlias(String alias) {
		for (DatasetProfile profile: datasetProfiles) {
			if(profile.getAlias().equals(alias)) return true;
		}
		return false;
	}
	
	public ArrayList<DatasetProfile> getProfiles(){
		return datasetProfiles;
	}
	
	public DatasetProfile getProfile(String alias) {
		for (DatasetProfile profile: datasetProfiles) {
			if (profile.getAlias().equals(alias)) return profile;
		}
		return null;
	}

}
