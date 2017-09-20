import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.JAVA_HOME;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;
import static org.apache.hadoop.yarn.api.records.LocalResourceType.FILE;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.*;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_APPLICATION_CLASSPATH;

public class NaiadYarnClient {
    private static final Logger LOG = Logger.getLogger(NaiadYarnClient.class.getName());

    private YarnConfiguration mYarnConf = new YarnConfiguration();
    private FileSystem mFileSystem = FileSystem.get(mYarnConf);
    private YarnClient mYarnClient = YarnClient.createYarnClient();
    private String mAppName = "NaiadOnYarnApp";
    private String mAppMasterJar = "";  // Jar that contains Application class

    private boolean mIsTest = false;

    private int mAppMasterMemory = 0;
    private int mNumVirtualCores = 0;
    private int mContainerMemory = 0;  // Memory that can be used by a container
    private int mAppPriority = 0;

    private int mPort = 2100;
    private String mProgram = "";
    private int mNumProcesses = 0;
    private int mNumThreads = 0;
    private ArrayList<Pair<String, Integer>> mHosts = new ArrayList<>();

    private String mLocalResourceHDFSPaths = "";  // Paths to resources that need to download to working environment
    private String mLogDir = "<LOG_DIR>";

    private ApplicationId mAppId = null;

    private NaiadYarnClient() throws IOException {
        mYarnClient.init(mYarnConf);
    }

    private int numProcessInHosts() {
        int num = 0;
        for (Pair<String, Integer> pair : mHosts) {
            num += pair.getSecond();
        }
        return num;
    }

    private boolean init(String[] args) throws ParseException, IOException {
        // parse options
        CommandLine cliParser = new GnuParser().parse(createClientOptions(), args);

        // analyze options
        if (args.length == 0 || cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        mAppName = cliParser.getOptionValue("app_name", mAppName);

        if (cliParser.hasOption("app_master")) {
            mAppMasterJar = cliParser.getOptionValue("app_master");
        } else {
            mAppMasterJar = JobConf.findContainingJar(NaiadApplicationMaster.class);
            if (mAppMasterJar == null) {
                throw new IllegalArgumentException("No jar specified for husky application master");
            }
        }
        LOG.info("The Application Master's jar is " + mAppMasterJar);

        mAppMasterMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "2048"));
        if (mAppMasterMemory <= 0) {
            throw new IllegalArgumentException(
                "Illegal memory specified for application master. Specified memory: " + mAppMasterMemory);
        }

        mContainerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
        if (mContainerMemory <= 0) {
            throw new IllegalArgumentException(
                "Illegal memory specified for container. Specified memory: " + mContainerMemory);
        }

        mNumVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        if (mNumVirtualCores <= 0) {
            throw new IllegalArgumentException(
                "Illegal number of virtual cores specified for container. Specified number of vcores: " + mNumVirtualCores);
        }

        mAppPriority = Integer.parseInt(cliParser.getOptionValue("app_priority", "1"));
        if (mAppPriority <= 0) {
            throw new IllegalArgumentException(
                "Illegal priority for husky application. Specified priority: " + mAppPriority);
        }

        mLocalResourceHDFSPaths = cliParser.getOptionValue("local_resource_dir", "hdfs:///naiad-yarn/");

        mLogDir = cliParser.getOptionValue("log_dir", mLogDir);

        if (!cliParser.hasOption("program")) {
            throw new IllegalArgumentException("The path to naiad program is not given.");
        }
        mProgram = cliParser.getOptionValue("program");

        if (cliParser.hasOption("hosts")) {
            String value = cliParser.getOptionValue("hosts");
            try {
                for (String pair : value.split(",")) {
                    String[] vals = pair.trim().split(":");
                    mHosts.add(new Pair(vals[0], Integer.valueOf(vals[1])));
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Illegal hosts specified. Format should be `host1:num1,host2:num2,...`. Specified hosts: " + value, e);
            }
        }

        if (cliParser.hasOption("num_process")) {
            mNumProcesses = Integer.parseInt(cliParser.getOptionValue("num_process"));
            if (mNumProcesses <= 0) {
                throw new IllegalArgumentException(
                    "Illegal number of processes specified. Specified number: " + mNumProcesses);
            }
            if (!mHosts.isEmpty() && mNumProcesses != numProcessInHosts()) {
                throw new IllegalArgumentException(
                    "Illegal number of processes specified, which is different from the number of hosts specified in `hosts`. " +
                        "Number of process given: " + numProcessInHosts() + ", number of hosts: " + mHosts.size());
            }
        } else {
            if (cliParser.hasOption("hosts")) {
                mNumProcesses = numProcessInHosts();
            } else {
                throw new IllegalArgumentException("Either `hosts` or `num_process` needs to be specified.");
            }
        }

        mNumThreads = Integer.parseInt(cliParser.getOptionValue("num_thread", "1"));
        if (mNumThreads <= 0) {
            throw new IllegalArgumentException(
                "Illegal number of threads specified. Specified number: " + mNumThreads);
        }

        mPort = Integer.parseInt(cliParser.getOptionValue("port", "2100"));
        if (!(mPort > 0 && mPort < 65536)) {
            throw new IllegalArgumentException(
                "Illegal port specified. Specified port: " + mPort);
        }

        mIsTest = cliParser.hasOption("test");

        return true;
    }

    private Map<String, String> getEnvironment() {
        String[] paths = mYarnConf.getStrings(YARN_APPLICATION_CLASSPATH, DEFAULT_YARN_APPLICATION_CLASSPATH);
        StringBuilder classpath = new StringBuilder();
        classpath.append("./*");
        for (String s : paths) {
            classpath.append(":").append(s);
        }
        return Collections.singletonMap("CLASSPATH", classpath.toString());
    }

    private boolean monitorApp() throws YarnException, IOException {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }

            ApplicationReport report = mYarnClient.getApplicationReport(mAppId);

            YarnApplicationState yarnState = report.getYarnApplicationState();
            FinalApplicationStatus appState = report.getFinalApplicationStatus();
            LOG.info("YarnState = " + yarnState + ", AppState = " + appState + ", Progress = " + report.getProgress());

            if (yarnState == FINISHED) {
                if (appState == SUCCEEDED) {
                    LOG.info("Application completed successfully.");
                    return true;
                } else {
                    LOG.info("Application completed unsuccessfully. YarnState: " + yarnState.toString() + ", ApplicationState: "
                        + appState.toString());
                    return false;
                }
            } else if (yarnState == KILLED || yarnState == FAILED) {
                LOG.info("Application did not complete. YarnState: " + yarnState.toString() + ", ApplicationState: "
                    + appState.toString());
                return false;
            }
        }
    }

    private HashMap<String, LocalResource> localResources = null;

    private Pair<String, LocalResource> constructLocalResource(String name, String path, LocalResourceType type)
        throws IOException {
        LOG.info("To copy " + name + "(" + path + ") from local file system");

        Path resourcePath = new Path(path);
        if (path.startsWith("hdfs://")) {
            FileStatus fileStatus = mFileSystem.getFileStatus(resourcePath);
            if (!fileStatus.isFile()) {
                throw new RuntimeException("Only files can be provided as local resources.");
            }
        } else {
            File file = new File(path);
            if (!file.exists()) {
                throw new RuntimeException("File not exist: " + path);
            }
            if (!file.isFile()) {
                throw new RuntimeException("Only files can be provided as local resources.");
            }
        }

        // if the file is not on hdfs, upload it to hdfs first.
        if (!path.startsWith("hdfs://")) {
            Path src = resourcePath;
            String newPath = mLocalResourceHDFSPaths + '/' + mAppName + '/' + mAppId + '/' + name;
            resourcePath = new Path(newPath);
            mFileSystem.copyFromLocalFile(false, true, src, resourcePath);
            LOG.info("Upload " + path + " to " + newPath);
            path = newPath;
        }

        FileStatus fileStatus = mFileSystem.getFileStatus(resourcePath);

        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setType(type);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath));
        resource.setTimestamp(fileStatus.getModificationTime());
        resource.setSize(fileStatus.getLen());

        return new Pair<>(path, resource);
    }

    private Map<String, LocalResource> getLocalResources() throws IOException {
        if (localResources == null) {
            localResources = new HashMap<>();

            Pair<String, LocalResource> resource = constructLocalResource("NaiadAppMaster.jar", mAppMasterJar, FILE);
            localResources.put("NaiadAppMaster.jar", resource.getSecond());

            mFileSystem.deleteOnExit(new Path(mLocalResourceHDFSPaths + '/' + mAppName + '/' + mAppId));
        }
        return localResources;
    }

    private boolean run() throws YarnException, IOException {
        mYarnClient.start();

        YarnClientApplication app = mYarnClient.createApplication();

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(mAppName);

        Resource resource = Records.newRecord(Resource.class);
        resource.setMemory(mAppMasterMemory);
        resource.setVirtualCores(mNumVirtualCores);
        appContext.setResource(resource);

        mAppId = appContext.getApplicationId();

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setLocalResources(getLocalResources());
        amContainer.setEnvironment(getEnvironment());

        StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append(JAVA_HOME.$()).append("/bin/java")
            .append(" -Xmx").append(mAppMasterMemory).append("m ")
            .append(NaiadApplicationMaster.class.getName())
            .append(" --app_id ").append(mAppId)
            .append(" --container_memory ").append(mContainerMemory)
            .append(" --container_vcores ").append(mNumVirtualCores)
            .append(" --app_priority ").append(mAppPriority)
            .append(" --app_master_log_dir ").append(mLogDir)
            .append(" --program '").append(mProgram).append('\'')
            .append(" --port ").append(mPort)
            .append(" --num_process ").append(mNumProcesses)
            .append(" --num_thread ").append(mNumThreads);
        if (!mHosts.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            for (Pair<String, Integer> pair : mHosts) {
                if (builder.length() != 0) {
                    builder.append(',');
                }
                builder.append(pair.getFirst()).append(':').append(pair.getSecond());
            }
            cmdBuilder.append(" --hosts ").append(builder.toString());
        }
        if (mIsTest) {
            cmdBuilder.append(" --test");
        }
        cmdBuilder.append(" 1>").append(mLogDir).append('/').append(mAppId).append('-').append("NaiadAppMaster.stdout")
            .append(" 2>").append(mLogDir).append('/').append(mAppId).append('-').append("NaiadAppMaster.stderr");

        amContainer.setCommands(Collections.singletonList(cmdBuilder.toString()));

        LOG.info("Command: " + amContainer.getCommands().get(0));

        appContext.setAMContainerSpec(amContainer);

        mYarnClient.submitApplication(appContext);

        return monitorApp();
    }

    private void printUsage() {
        new HelpFormatter().printHelp("NaiadYarnClient", createClientOptions());
    }

    private Options createClientOptions() {
        Options opts = new Options();
        // set up options
        opts.addOption("help", false, "Print Usage");

        opts.addOption("app_name", true, "The name of the application");

        opts.addOption("app_master", true, "Local path to the jar file of application master.");

        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run application master");
        opts.addOption("container_memory", true,
            "Amount of memory in MB to be requested to run container. Each container is a worker node.");
        opts.addOption("container_vcores", true, "Number of virtual cores that a container can use");
        opts.addOption("app_priority", true, "A number to indicate the priority of the husky application");

        opts.addOption("program", true, "Location of naiad executable");
        opts.addOption("port", true, "A starting port for naiad process. The port value will be increased automatically for the processes in the same host");
        opts.addOption("num_process", true,
            "Number of naiad processes. Either num_process or hosts should be declared");
        opts.addOption("hosts", true,
            "Desired hosts and number of processes of each host. Format(split by comma): host1:num1,host2:num2,host3:num3,... Either num_process or hosts should be declared");
        opts.addOption("num_thread", true, "Number of threads in each naiad process");

        opts.addOption("local_resource_dir", true, "Where to store resources so that containers can access");

        opts.addOption("log_dir", true, "Directory to store logs of application master and worker containers");
        opts.addOption("test", false,
            "If given, containers will be requested and created, naiad program will not run but the command to run the program will be written to stdout.");

        return opts;
    }

    public static void main(String[] args) {
        LOG.info("Start running NaiadYarnClient");
        boolean result = false;
        try {
            NaiadYarnClient client = new NaiadYarnClient();
            try {
                // argument initialization errors
                if (!client.init(args)) {
                    System.exit(0);
                }
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Exception on parsing arguments", e);
                client.printUsage();
                System.exit(-1);
            }
            // runtime exceptions
            result = client.run();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Exception on running application master", e);
            e.printStackTrace();
            System.exit(-1);
        }
        if (!result) {
            System.exit(-1);
        }
        System.exit(0);
    }
}
