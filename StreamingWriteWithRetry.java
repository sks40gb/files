import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import io.delta.tables.DeltaTable;
import io.delta.exceptions.ConcurrentAppendException;
import io.delta.exceptions.ConcurrentDeleteReadException;
import io.delta.exceptions.ConcurrentDeleteDeleteException;

import java.util.concurrent.TimeUnit;

public class StreamingWriteWithRetry {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Streaming Write with Retry")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        // Example streaming DataFrame (replace with your actual streaming DataFrame)
        Dataset<Row> stream = spark.readStream().format("rate").option("rowsPerSecond", "1").load();

        // Example usage of the streaming write with retry logic
        try {
            streamingWriteWithRetry(stream, 3, false, "delta_table_name", null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        spark.stop();
    }

    public static void streamingWriteWithRetry(Dataset<Row> stream, int maxAttempts, boolean indefinite, String table, String path) throws Exception {
        int attempt = 0;

        while (true) {
            try {
                StreamingQuery query;
                if (table != null) {
                    query = stream.writeStream()
                            .format("delta")
                            .option("checkpointLocation", "path/to/checkpoint")
                            .toTable(table);
                } else if (path != null) {
                    query = stream.writeStream()
                            .format("delta")
                            .option("checkpointLocation", "path/to/checkpoint")
                            .start(path);
                } else {
                    throw new IllegalArgumentException("Either table or path must be provided.");
                }

                query.awaitTermination();
                return;

            } catch (StreamingQueryException e) {
                Throwable cause = e.getCause();
                if (cause instanceof ConcurrentAppendException ||
                    cause instanceof ConcurrentDeleteReadException ||
                    cause instanceof ConcurrentDeleteDeleteException) {

                    attempt++;

                    if (!indefinite && attempt >= maxAttempts) {
                        throw e;
                    }

                    long sleepTime = (long) Math.min(120, Math.pow(2, attempt));
                    System.out.println("Attempt " + attempt + " failed due to concurrent modification. Retrying in " + sleepTime + " seconds...");
                    TimeUnit.SECONDS.sleep(sleepTime);
                } else {
                    throw e;
                }
            }
        }
    }
}
