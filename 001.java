import java.io.*;
import java.nio.file.*;
import java.util.*;

/*
Explanation
Argument Handling: The script takes job arguments, which are passed to the Java application via args.
Environment Variables: The script uses environment variables such as
*/    
public class LockUnlockSnapshots {

    public static void main(String[] args) throws IOException, InterruptedException {
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

        executeCommand("hdfs dfs -get @APP_STORAGE_URL@/glx/keytab/" + user + "/" + user + ".keytab");

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
                        executeCommand("hadoop fs -rm -f " + optSnapPath + "/_UNLOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + optSnapPath + "/_LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + optSnapPath + "/_DELTA_START_STATUS");
                        executeCommand("hadoop fs -rm -f " + optSnapPath + "/_DELTA_END_STATUS");
                        executeCommand("hadoop fs -touchz " + optSnapPath + "/_DELTA_START_STATUS");
                        executeCommand("hadoop fs -touchz " + optSnapPath + "/_DELTA_END_STATUS");
                    }
                    if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                        System.out.println("LOCK " + " future snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + futSnapPath + "/_UNLOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + futSnapPath + "/_LOCK_STATIC");
                    }
                } else if ("UNLOCK".equals(status)) {
                    System.out.println("######################## Starting UNLOCK [ " + i + " ] snapshot ");
                    if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                        System.out.println("UNLOCK option snapshot  :  create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + optSnapPath + "/_LOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + optSnapPath + "/_UNLOCK_STATIC");
                    }
                    if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                        System.out.println("UNLOCK future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + futSnapPath + "/_LOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + futSnapPath + "/_UNLOCK_STATIC");
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
                            executeCommand("hadoop fs -rm -f " + optSnapPath + "/_UNLOCK_STATIC");
                            executeCommand("hadoop fs -touchz " + optSnapPath + "/_LOCK_STATIC");
                        } else if (lockDelta.contains("|" + i + "|")) {
                            executeCommand("hadoop fs -rm -f " + optSnapPath + "/_UNLOCK_DELTA");
                            executeCommand("hadoop fs -touchz " + optSnapPath + "/_LOCK_DELTA");
                            executeCommand("echo " + i + " | hadoop fs -appendToFile - " + optSnapPath + "/_DELTA_START_STATUS");
                        }
                    }
                    if (instrumentsMatch.contains("FUTURE") && lockProvider.contains("|" + i + "|")) {
                        System.out.println("LOCK future snapshot : check if it is not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + futSnapPath + "/_UNLOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + futSnapPath + "/_LOCK_STATIC");
                    }
                } else if ("UNLOCK".equals(status)) {
                    System.out.println("######################## Starting UNLOCK [ " + i + " ] snapshot ");
                    if (instrumentsMatch.contains("OPTION")) {
                        System.out.println("UNLOCK option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        if (lockProvider.contains("|" + i + "|")) {
                            executeCommand("hadoop fs -rm -f " + optSnapPath + "/_LOCK_STATIC");
                            executeCommand("hadoop fs -touchz " + optSnapPath + "/_UNLOCK_STATIC");
                        } else if (lockDelta.contains("|" + i + "|")) {
                            executeCommand("echo " + i + " | hadoop fs -appendToFile - " + optSnapPath + "/_DELTA_END_STATUS");

                            String deltaStartStatus = executeCommand("hadoop fs -cat " + optSnapPath + "/_DELTA_START_STATUS | sort");
                            String deltaEndStatus = executeCommand("hadoop fs -cat " + optSnapPath + "/_DELTA_END_STATUS | sort");

                            if (deltaStartStatus.trim().equals(deltaEndStatus.trim())) {
                                executeCommand("hadoop fs -rm -f " + optSnapPath + "/_LOCK_DELTA");
                                executeCommand("hadoop fs -touchz " + optSnapPath + "/_UNLOCK_DELTA");
                            }
                        }
                    }
                    if (instrumentsMatch.contains("FUTURE") && lockProvider.contains("|" + i + "|")) {
                        System.out.println("UNLOCK future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + futSnapPath + "/_LOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + futSnapPath + "/_UNLOCK_STATIC");
                    }
                }
            }
        }
        System.exit(exitCode);
    }

    private static String executeCommand(String command) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command);
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        process.waitFor();
        if (process.exitValue() != 0) {
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            StringBuilder errorOutput = new StringBuilder();
            while ((line = errorReader.readLine()) != null) {
                errorOutput.append(line).append("\n");
            }
            System.err.println("Error executing command: " + command);
            System.err.println(errorOutput.toString());
            System.exit(process.exitValue());
        }
        return output.toString();
    }
}
