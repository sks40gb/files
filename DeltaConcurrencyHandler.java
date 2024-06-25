import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import io.delta.tables.DeltaTable;
import io.delta.exceptions.ConcurrentAppendException;
import io.delta.exceptions.ConcurrentDeleteReadException;
import io.delta.exceptions.ConcurrentDeleteDeleteException;

import java.util.concurrent.TimeUnit;

public class DeltaConcurrencyHandler {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DeltaConcurrencyHandler")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        // Example DataFrame (replace with your actual DataFrame)
        Dataset<Row> df = spark.read().format("parquet").load("path/to/parquet/file");

        // Example usage of the batch write with retry logic
        try {
            writeWithRetry(df, 3, false, "path/to/delta/table");
        } catch (Exception e) {
            e.printStackTrace();
        }

        spark.stop();
    }

    public static void writeWithRetry(Dataset<Row> df, int maxAttempts, boolean indefinite, String path) throws Exception {
        int attempt = 0;

        while (true) {
            try {
                df.write().format("delta").mode("append").save(path);
                return;

            } catch (ConcurrentAppendException | ConcurrentDeleteReadException | ConcurrentDeleteDeleteException e) {
                attempt++;

                if (!indefinite && attempt >= maxAttempts) {
                    throw e;
                }

                long sleepTime = (long) Math.min(120, Math.pow(2, attempt));
                System.out.println("Attempt " + attempt + " failed due to concurrent modification. Retrying in " + sleepTime + " seconds...");
                TimeUnit.SECONDS.sleep(sleepTime);
            }
        }
    }
}
