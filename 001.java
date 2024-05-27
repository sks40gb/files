import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

/*
Explanation:
Argument Handling:

String.join(" ", args) prints all arguments.
Arguments are assigned to variables.
Debug Mode:

Checks for the DEBUG_MODE environment variable and prints a message if enabled.
Current User:

Gets the current user using System.getenv("USER").
Kerberos Arguments:

Variables for keytab path and user principal.
*/

public class ShellScriptEquivalent {

    public static void main(String[] args) {
        // Echo all arguments
        System.out.println(String.join(" ", args));

        int exitCode = 0;
        boolean debugMode = System.getenv("DEBUG_MODE") != null && System.getenv("DEBUG_MODE").equals("true");

        if (debugMode) {
            System.out.println("Debug mode enabled");
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

        // Get current user
        String user = System.getenv("USER");
        System.out.println("current unix user: " + user);

        if ("null".equals(lockInstrument)) {
            System.out.println("################ LOCK snapshot not needed for " + provider + " process");
            System.exit(0);
        }

        // Kerberos arguments
        String kerberosKeytabPath = "@spark.history.kerberos.keytab@";
        String kerberosUserPrincipal = "@spark.history.kerberos.principal@";

        // Fetch keytab file from HDFS
        executeCommand("hdfs dfs -get @APP_STORAGE_URL@/glx/keytab/" + user + "/" + user + ".keytab");

        // Split providers and matches
        String[] arrProvider = provider.split("#");
        String[] arrProvidersMatch = providersMatch.split(",");
        String[] arrInstrumentsMatch = instrumentsMatch.split(",");

        // Snapshot paths
        String optSnapPath = "@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prx_format/option/option_snapshot_flag";
        String futSnapPath = "@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prx_format/future/future_snapshot_flag";

        // LOCK / UNLOCK in feeding module
        for (String i : arrProvider) {
            if (i.matches(lockInstrument)) {
                if ("LOCK".equals(status)) {
                    System.out.println("############### Starting LOCK [" + i + "} snapshot ");

                    if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                        System.out.println("LOCK option snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC and _UNLOCK_STATIC");
                        executeHadoopCommands(optSnapPath, "LOCK");
                    }

                    if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                        System.out.println("LOCK future snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        executeHadoopCommands(futSnapPath, "LOCK");
                    }
                } else if ("UNLOCK".equals(status)) {
                    System.out.println("###################### Starting UNLOCK [" + i + "] snapshot ");

                    if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                        System.out.println("UNLOCK option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                        executeHadoopCommands(optSnapPath, "UNLOCK");
                    }
                }
            }
        }

        // LOCK / UNLOCK in updating module and delta processing
        for (String i : arrProvidersMatch) {
            if (lockProvider.contains("|" + i + "|") || lockDelta.contains("|" + i + "|")) {
                if ("LOCK".equals(status)) {
                    System.out.println("############################### Starting LOCK [" + i + "] snapshot ");

                    if (instrumentsMatch.contains("OPTION")) {
                        System.out.println("LOCK option snapshot : check if it not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");

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
                        System.out.println("LOCK future snapshot : check if it not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");
                        executeCommand("hadoop fs -rm -f " + futSnapPath + "/_UNLOCK_STATIC");
                        executeCommand("hadoop fs -touchz " + futSnapPath + "/_LOCK_STATIC");
                    }
                } else if ("UNLOCK".equals(status)) {
                    System.out.println("######################### Starting UNLOCK [" + i + "] snapshot ");

                    if (instrumentsMatch.contains("OPTION")) {
                        System.out.println("UNLOCK option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");

                        if (lockProvider.contains("|" + i + "|")) {
                            executeCommand("hadoop fs -rm -f " + optSnapPath + "/_LOCK_STATIC");
                            executeCommand("hadoop fs -touchz " + optSnapPath + "/_UNLOCK_STATIC");
                        } else if (lockDelta.contains("|" + i + "|")) {
                            executeCommand("echo " + i + " | hadoop fs -appendToFile - " + optSnapPath + "/_DELTA_END_STATUS");

                            String deltaStartStatus = executeCommandWithOutput("hadoop fs -cat " + optSnapPath + "/_DELTA_START_STATUS | sort");
                            String deltaEndStatus = executeCommandWithOutput("hadoop fs -cat " + optSnapPath + "/_DELTA_END_STATUS | sort");

                            if (deltaStartStatus.replaceAll("\\s+", "").equals(deltaEndStatus.replaceAll("\\s+", ""))) {
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

    private static void executeCommand(String command) {
        try {
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static String executeCommandWithOutput(String command) {
        StringBuilder output = new StringBuilder();
        try {
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return output.toString();
    }

    private static void executeHadoopCommands(String snapPath, String action) {
        if ("LOCK".equals(action)) {
            executeCommand("hadoop fs -rm -f " + snapPath + "/_UNLOCK_STATIC");
            executeCommand("hadoop fs -touchz " + snapPath + "/_LOCK_STATIC");
            executeCommand("hadoop fs -rm -f " + snapPath + "/_DELTA_START_STATUS");
            executeCommand("hadoop fs -rm -f " + snapPath + "/_DELTA_END_STATUS");
            executeCommand("hadoop fs -touchz " + snapPath + "/_DELTA_START_STATUS");
            executeCommand("hadoop fs -touchz " + snapPath + "/_DELTA_END_STATUS");
        } else if ("UNLOCK".equals(action)) {
            executeCommand("hadoop fs -rm -f " + snapPath + "/_LOCK_STATIC");
            executeCommand("hadoop fs -touchz " + snapPath + "/_UNLOCK_STATIC");
        }
    }
}
