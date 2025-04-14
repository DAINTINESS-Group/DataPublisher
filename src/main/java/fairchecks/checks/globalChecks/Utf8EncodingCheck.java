package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class Utf8EncodingCheck implements IInteroperabilityCheck {

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
    		String uriPath = dataset.inputFiles()[0];
    		
    		URI uri = new URI(uriPath);
    		String filePath = Paths.get(uri).toString();
            
            try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
            	byte[] buffer = new byte[fileInputStream.available()];
            	DataInputStream dataInputStream = new DataInputStream(fileInputStream);
            	dataInputStream.readFully(buffer);            
                
                boolean valid = isValidUtf8(buffer);
                return valid;
            }

        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
            return false;
        } catch (URISyntaxException e) {
			e.printStackTrace();
			return false;
		}
    }
    
    private boolean isValidUtf8(byte[] input) {
        int i = 0;
        while (i < input.length) {
            int byte1 = input[i] & 0xFF;

            if ((byte1 & 0x80) == 0) {
                // 1-byte (ASCII)
                i++;
            } else if ((byte1 & 0xE0) == 0xC0) {
                // 2-byte
                if (i + 1 >= input.length) return false;
                if ((input[i + 1] & 0xC0) != 0x80) return false;
                i += 2;
            } else if ((byte1 & 0xF0) == 0xE0) {
                // 3-byte
                if (i + 2 >= input.length) return false;
                if ((input[i + 1] & 0xC0) != 0x80 || (input[i + 2] & 0xC0) != 0x80) return false;
                i += 3;
            } else if ((byte1 & 0xF8) == 0xF0) {
                // 4-byte
                if (i + 3 >= input.length) return false;
                if ((input[i + 1] & 0xC0) != 0x80 || (input[i + 2] & 0xC0) != 0x80 || (input[i + 3] & 0xC0) != 0x80)
                    return false;
                i += 4;
            } else {
                return false;
            }
        }
        return true;
    }
    
}
