/*

Explanation:
SnapshotOperation: The abstract base class defines the common file operations and an abstract execute method.
LockOperation: A subclass of SnapshotOperation that implements the locking logic.
UnlockOperation: A subclass of SnapshotOperation that implements the unlocking logic.
LockUnlockSnapshots: The main class that initializes the appropriate subclass based on the status and calls the execute method.
This design uses polymorphism to separate the logic for locking and unlocking snapshots into different classes, making the code more modular and easier to maintain.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public abstract class SnapshotOperation {
    protected Configuration conf;
    protected FileSystem fs;
    protected String optSnapPath;
    protected String futSnapPath;

    public SnapshotOperation(Configuration conf, String optSnapPath, String futSnapPath) throws IOException {
        this.conf = conf;
        this.fs = FileSystem.get(URI.create(optSnapPath), conf);
        this.optSnapPath = optSnapPath;
        this.futSnapPath = futSnapPath;
    }

    protected void deleteFileIfExists(Path path) throws IOException {
        if (fs.exists(path)) {
            fs.delete(path, false);
        }
    }

    protected void createFile(Path path) throws IOException {
        FSDataOutputStream out = fs.create(path);
        out.close();
    }

    protected void appendToFile(Path path, String content) throws IOException {
        FSDataOutputStream out = fs.append(path);
        out.writeUTF(content);
        out.close();
    }

    protected String readAndSortFile(Path path) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        List<String> lines = br.lines().collect(Collectors.toList());
        br.close();
        return lines.stream().sorted().collect(Collectors.joining("\n"));
    }

    public abstract void execute(String provider, List<String> arrProvider, List<String> arrProvidersMatch, String lockProvider, String lockInstrument, String lockDelta, String instrumentsMatch, String status) throws IOException;
}


import java.io.IOException;
        import java.util.List;

public class LockOperation extends SnapshotOperation {

    public LockOperation(Configuration conf, String optSnapPath, String futSnapPath) throws IOException {
        super(conf, optSnapPath, futSnapPath);
    }

    @Override
    public void execute(String provider, List<String> arrProvider, List<String> arrProvidersMatch, String lockProvider, String lockInstrument, String lockDelta, String instrumentsMatch, String status) throws IOException {
        // LOCK in feeding module
        for (String i : arrProvider) {
            if (i.matches(lockInstrument)) {
                System.out.println("######################## Starting LOCK [ " + i + " ] snapshot ");
                if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                    System.out.println("LOCK option snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC and _UNLOCK_STATIC");
                    deleteFileIfExists(new Path(optSnapPath + "/_UNLOCK_STATIC"));
                    createFile(new Path(optSnapPath + "/_LOCK_STATIC"));
                    deleteFileIfExists(new Path(optSnapPath + "/_DELTA_START_STATUS"));
                    deleteFileIfExists(new Path(optSnapPath + "/_DELTA_END_STATUS"));
                    createFile(new Path(optSnapPath + "/_DELTA_START_STATUS"));
                    createFile(new Path(optSnapPath + "/_DELTA_END_STATUS"));
                }
                if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                    System.out.println("LOCK future snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC");
                    deleteFileIfExists(new Path(futSnapPath + "/_UNLOCK_STATIC"));
                    createFile(new Path(futSnapPath + "/_LOCK_STATIC"));
                }
            }
        }

        // LOCK in updating module and delta processing
        for (String i : arrProvidersMatch) {
            if (lockProvider.contains("|" + i + "|") || lockDelta.contains("|" + i + "|")) {
                System.out.println("######################## Starting LOCK [ " + i + " ] snapshot ");
                if (instrumentsMatch.contains("OPTION")) {
                    System.out.println("LOCK option snapshot : check if it is not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");
                    if (lockProvider.contains("|" + i + "|")) {
                        deleteFileIfExists(new Path(optSnapPath + "/_UNLOCK_STATIC"));
                        createFile(new Path(optSnapPath + "/_LOCK_STATIC"));
                    } else if (lockDelta.contains("|" + i + "|")) {
                        deleteFileIfExists(new Path(optSnapPath + "/_UNLOCK_DELTA"));
                        createFile(new Path(optSnapPath + "/_LOCK_DELTA"));
                        appendToFile(new Path(optSnapPath + "/_DELTA_START_STATUS"), i);
                    }
                }
                if (instrumentsMatch.contains("FUTURE") && lockProvider.contains("|" + i + "|")) {
                    System.out.println("LOCK future snapshot : check if it is not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC");
                    deleteFileIfExists(new Path(futSnapPath + "/_UNLOCK_STATIC"));
                    createFile(new Path(futSnapPath + "/_LOCK_STATIC"));
                }
            }
        }
    }
}


import java.io.IOException;
        import java.util.List;

public class UnlockOperation extends SnapshotOperation {

    public UnlockOperation(Configuration conf, String optSnapPath, String futSnapPath) throws IOException {
        super(conf, optSnapPath, futSnapPath);
    }

    @Override
    public void execute(String provider, List<String> arrProvider, List<String> arrProvidersMatch, String lockProvider, String lockInstrument, String lockDelta, String instrumentsMatch, String status) throws IOException {
        // UNLOCK in feeding module
        for (String i : arrProvider) {
            if (i.matches(lockInstrument)) {
                System.out.println("######################## Starting UNLOCK [ " + i + " ] snapshot ");
                if ("LD_OPTION".equals(i) || "ALL".equals(i)) {
                    System.out.println("UNLOCK option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                    deleteFileIfExists(new Path(optSnapPath + "/_LOCK_STATIC"));
                    createFile(new Path(optSnapPath + "/_UNLOCK_STATIC"));
                }
                if ("LD_FUTURE".equals(i) || "ALL".equals(i)) {
                    System.out.println("UNLOCK future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                    deleteFileIfExists(new Path(futSnapPath + "/_LOCK_STATIC"));
                    createFile(new Path(futSnapPath + "/_UNLOCK_STATIC"));
                }
            }
        }

        // UNLOCK in updating module and delta processing
        for (String i : arrProvidersMatch) {
            if (lockProvider.contains("|" + i + "|") || lockDelta.contains("|" + i + "|")) {
                System.out.println("######################## Starting UNLOCK [ " + i + " ] snapshot ");
                if (instrumentsMatch.contains("OPTION")) {
                    System.out.println("UNLOCK option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                    if (lockProvider.contains("|" + i + "|")) {
                        deleteFileIfExists(new Path(optSnapPath + "/_LOCK_STATIC"));
                        createFile(new Path(optSnapPath + "/_UNLOCK_STATIC"));
                    } else if (lockDelta.contains("|" + i + "|")) {
                        appendToFile(new Path(optSnapPath + "/_DELTA_END_STATUS"), i);
                        String deltaStartStatus = readAndSortFile(new Path(optSnapPath + "/_DELTA_START_STATUS"));
                        String deltaEndStatus = readAndSortFile(new Path(optSnapPath + "/_DELTA_END_STATUS"));
                        if (deltaStartStatus.equals(deltaEndStatus)) {
                            deleteFileIfExists(new Path(optSnapPath + "/_LOCK_DELTA"));
                            createFile(new Path(optSnapPath + "/_UNLOCK_DELTA"));
                        }
                    }
                }
                if (instrumentsMatch.contains("FUTURE") && lockProvider.contains("|" + i + "|")) {
                    System.out.println("UNLOCK future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC");
                    deleteFileIfExists(new Path(futSnapPath + "/_LOCK_STATIC"));
                    createFile(new Path(futSnapPath + "/_UNLOCK_STATIC"));
                }
            }
        }
    }
}


import org.apache.hadoop.conf.Configuration;

        import java.io.IOException;
        import java.util.Arrays;
        import java.util.List;

public class LockUnlockSnapshots {

    public static void main(String[] args) throws IOException {
        // Print all arguments
        System.out.println(String.join(" ", args));

        int exitCode = 0;

        String debugMode = System.getenv("DEBUG_MODE");
        if ("true".equals(debugMode)) {
            // Enable debug mode
            System.out.println("Debug mode is ON");
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

        // Initialize Hadoop configuration
        Configuration conf = new Configuration();

        // OPTION/FUTURE snapshots paths (these need to be replaced with actual values or fetched from config)
        String optSnapPath = "@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prk_format/option/option_snapshot_flag";
        String futSnapPath = "@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prk_format/future/future_snapshot_flag";

        List<String> arrProvider = Arrays.asList(provider.split("#"));
        List<String> arrProvidersMatch = Arrays.asList(providersMatch.split(","));
        List<String> arrInstrumentsMatch = Arrays.asList(instrumentsMatch.split(","));

        SnapshotOperation operation;
        if ("LOCK".equals(status)) {
            operation = new LockOperation(conf, optSnapPath, futSnapPath);
        } else if ("UNLOCK".equals(status)) {
            operation = new UnlockOperation(conf, optSnapPath, futSnapPath);
        } else {
            throw new IllegalArgumentException("Invalid status: " + status);
        }

        operation.execute(provider, arrProvider, arrProvidersMatch, lockProvider, lockInstrument, lockDelta, instrumentsMatch, status);

        System.exit(exitCode);
    }
}



