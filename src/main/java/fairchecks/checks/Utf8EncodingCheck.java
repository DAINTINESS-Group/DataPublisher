package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Utf8EncodingCheck implements IInteroperabilityCheck {

    private final List<String> invalidRows = new ArrayList<>();

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
            String filePath = dataset.sparkSession().conf().get("spark.sql.csv.filepath");
            if (filePath.isEmpty()) {
                System.err.println("No file path provided in Spark config.");
                invalidRows.add("No file path provided in Spark config.");
                return false;
            }

            byte[] bytes = readAllBytes(filePath);
            
            boolean isUtf8 = isValidUtf8(bytes);
            System.out.println("UTF-8 Validation Result → " + isUtf8);

            if (!isUtf8) {
                invalidRows.add("File contains invalid UTF-8 byte sequences.");
                return false;
            }

            return true;

        } catch (Exception e) {
            System.err.println("❌ Error checking UTF-8 encoding: " + e.getMessage());
            invalidRows.add("Error while checking encoding: " + e.getMessage());
            return false;
        }
    }

    private byte[] readAllBytes(String filePath) throws Exception {
        try (InputStream is = new FileInputStream(filePath)) {
            return is.readAllBytes();
        }
    }

    private boolean isValidUtf8(byte[] bytes) {
    	try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);

            decoder.decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
}
