import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class 001_V1 {

    public static void main(String[] args) throws IOException {
        for (String arg : args) {
            System.out.println(arg);
        }
        int exitCode = 0;

        String debugMode = System.getenv("DEBUG_MODE");
        if ("true".equals(debugMode)) {
            // Enable debug mode
            System.setProperty("sun.java.command", "-Djava.util.logging.ConsoleHandler.level=FINEST");
        }

        // Job arguments
        String provider = args[0];
        String providersMatch = args[1];
        String instrumentsMatch = args[2];
        String runMode = args[3];
        String lockProvider = args[4];
        String lockInstrument = args[5];
        String lockDelta = args[6];
        String status = args[7];

        String user = System.getenv("USER");
        System.out.println("current unix user: " + user);

        if ("null".equals(lockInstrument)) {
            System.out.println("######################## LOCK snapshot not needed for " + provider + " process");
            System.exit(0);
        }

        // Kinit arguments (these need to be replaced with actual values or fetched from config)
        String kerberosKeytabPath = "@spark.history.kerberos.keytab@";
        String kerberosUserPrincipal = "@spark.history.kerberos.principal@";

        // Initialize Hadoop configuration
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("@APP_STORAGE_URL@"), conf);

        // Get keytab file
        Path keytabSource = new Path("@APP_STORAGE_URL@/glx/keytab/" + user + "/" + user + ".keytab");
        Path keytabDestination = new Path("/home/" + user + "/" + user + ".keytab");
        fs.copyToLocalFile(keytabSource, keytabDestination);

        List<String> arrProvider = Arrays.asList(provider.split("#"));
        List<String> arrProvidersMatch = Arrays.asList(providersMatch.split(","));
        List<String> arrInstrumentsMatch = Arrays.asList(instrumentsMatch.split(","));

        // OPTION/FUTURE snapshots paths (these need to be replaced with actual values or fetched from config)
        String optSnapPath = "@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prk_format/option/option_snapshot_flag";
        String futSnapPath = "@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prk_format/future/future_snapshot_flag";

        // LOCK / UNLOCK in feeding module
        for (String i : arrProvider) {
            if (i.matches(lockInstrument)) {
                if ("LOCK".equals(status)) {
                    System.out.println("######################## Starting LOCK [ " + i + " ] snapshot ");
                    if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                        System.out.println("LOCK " + " option snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC and _UNLOCK_STATIC");
                        deleteFileIfExists(fs, new Path(optSnapPath + "/_UNLOCK_STATIC"));
                        createFile(fs, new Path(optSnapPath + "/_LOCK_STATIC"));
                        deleteFileIfExists(fs, new Path(optSnapPath + "/_DELTA_START_STATUS"));
                        deleteFileIfExists(fs, new Path(optSnapPath + "/_DELTA_END_STATUS"));
                        createFile(fs, new Path(optSnapPath + "/_DELTA_START_STATUS"));
                        createFile(fs, new Path(optSnapPath + "/_DELTA_END_STATUS"));
                    }
                    if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                        System.out.println("LOCK " + " future snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        deleteFileIfExists(fs, new Path(futSnapPath + "/_UNLOCK_STATIC"));
                        createFile(fs, new Path(futSnapPath + "/_LOCK_STATIC"));
                    }
                } else if ("UNLOCK".equals(status)) {
                    System.out.println("######################## Starting UNLOCK [ " + i + " ] snapshot ");
                    if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                        System.out.println("UNLOCK option snapshot  :  create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        deleteFileIfExists(fs, new Path(optSnapPath + "/_LOCK_STATIC"));
                        createFile(fs, new Path(optSnapPath + "/_UNLOCK_STATIC"));
                    }
                    if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                        System.out.println("UNLOCK future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        deleteFileIfExists(fs, new Path(futSnapPath + "/_LOCK_STATIC"));
                        createFile(fs, new Path(futSnapPath + "/_UNLOCK_STATIC"));
                    }
                }
            }
        }

        // LOCK / UNLOCK in updating module and delta processing
        for (String i : arrProvidersMatch) {
            if (lockProvider.contains("|" + i + "|") || lockDelta.contains("|" + i + "|")) {
                if ("LOCK".equals(status)) {
                    System.out.println("######################## Starting LOCK [ " + i + " ] snapshot ");
                    if (instrumentsMatch.contains("OPTION")) {
                        System.out.println("LOCK option snapshot : check if it is not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        if (lockProvider.contains("|" + i + "|")) {
                            deleteFileIfExists(fs, new Path(optSnapPath + "/_UNLOCK_STATIC"));
                            createFile(fs, new Path(optSnapPath + "/_LOCK_STATIC"));
                        } else if (lockDelta.contains("|" + i + "|")) {
                            deleteFileIfExists(fs, new Path(optSnapPath + "/_UNLOCK_DELTA"));
                            createFile(fs, new Path(optSnapPath + "/_LOCK_DELTA"));
                            appendToFile(fs, new Path(optSnapPath + "/_DELTA_START_STATUS"), i);
                        }
                    }
                    if (instrumentsMatch.contains("FUTURE") && lockProvider.contains("|" + i + "|")) {
                        System.out.println("LOCK future snapshot : check if it is not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        deleteFileIfExists(fs, new Path(futSnapPath + "/_UNLOCK_STATIC"));
                        createFile(fs, new Path(futSnapPath + "/_LOCK_STATIC"));
                    }
                } else if ("UNLOCK".equals(status)) {
                    System.out.println("######################## Starting UNLOCK [ " + i + " ] snapshot ");
                    if (instrumentsMatch.contains("OPTION")) {
                        System.out.println("UNLOCK option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        if (lockProvider.contains("|" + i + "|")) {
                            deleteFileIfExists(fs, new Path(optSnapPath + "/_LOCK_STATIC"));
                            createFile(fs, new Path(optSnapPath + "/_UNLOCK_STATIC"));
                        } else if (lockDelta.contains("|" + i + "|")) {
                            appendToFile(fs, new Path(optSnapPath + "/_DELTA_END_STATUS"), i);
                            String deltaStartStatus = readAndSortFile(fs, new Path(optSnapPath + "/_DELTA_START_STATUS"));
                            String deltaEndStatus = readAndSortFile(fs, new Path(optSnapPath + "/_DELTA_END_STATUS"));
                            if (deltaStartStatus.equals(deltaEndStatus)) {
                                deleteFileIfExists(fs, new Path(optSnapPath + "/_LOCK_DELTA"));
                                createFile(fs, new Path(optSnapPath + "/_UNLOCK_DELTA"));
                            }
                        }
                    }
                    if (instrumentsMatch.contains("FUTURE") && lockProvider.contains("|" + i + "|")) {
                        System.out.println("UNLOCK future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        deleteFileIfExists(fs, new Path(futSnapPath + "/_LOCK_STATIC"));
                        createFile(fs, new Path(futSnapPath + "/_UNLOCK_STATIC"));
                    }
                }
            }
        }
        System.exit(exitCode);
    }

    private static void deleteFileIfExists(FileSystem fs, Path path) throws IOException {
        if (fs.exists(path)) {
            fs.delete(path, false);
        }
    }

    private static void createFile(FileSystem fs, Path path) throws IOException {
        fs.create(path).close();
    }

    private static void appendToFile(FileSystem fs, Path path, String content) throws IOException {
        FSDataOutputStream out = fs.append(path);
        out.writeUTF(content);
        out.close();
    }

    private static String readAndSortFile(FileSystem fs, Path path) throws IOException {
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            content.append(line).append("\n");
        }
        br.close();
        in.close();
        return Arrays.stream(content.toString().split("\n"))
                .sorted()
                .reduce((s1, s2) -> s1 + "\n" + s2)
                .orElse("");
    }
