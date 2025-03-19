package fairchecks.checks;

import fairchecks.api.IAccessibilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.HttpURLConnection;
import java.net.URL;

public class UrlAccessibilityCheck implements IAccessibilityCheck {
	
	@Override
    public String getCheckId() {
        return "AEU2";
    }

    @Override
    public String getCheckDescription() {
        return "Data to be provided from an accessible download URL.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String downloadUrl = dataset.sparkSession().conf().get("data.download.url", "");

            if (downloadUrl.isEmpty()) {
                return false;
            }
            URL url = new URL(downloadUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.connect();

            int responseCode = connection.getResponseCode();
            return responseCode == 200;

        } catch (Exception e) {
            System.err.println("Error executing Download URL Accessibility Check: " + e.getMessage());
            return false;
        }
    }

}
