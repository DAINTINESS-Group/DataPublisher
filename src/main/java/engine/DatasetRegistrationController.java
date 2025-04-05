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
			
			//Added 2nd option
			Dataset<Row> df = spark.read().option("header", hasHeader)
					.option("inferSchema", true)
					.csv(path)
					.withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));
			//df.printSchema();
			//NEW INFO
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
