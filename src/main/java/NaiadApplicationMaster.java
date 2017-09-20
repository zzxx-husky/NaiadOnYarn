import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NaiadApplicationMaster {

    private static final Logger LOG = Logger.getLogger(NaiadApplicationMaster.class.getName());

    private YarnConfiguration mYarnConf = new YarnConfiguration();

    private String mAppId = "Unknown";

    private String mAppMasterLogDir = "";

    private int mContainerMemory = 0;
    private int mNumVirtualCores = 0;
    private int mAppPriority = 0;

    private boolean mIsTest = false;
    private String mProgram = "";
    private int mPort = 0;
    private int mNumProcesses = 0;
    private int mNumThreads = 0;
    private ArrayList<Pair<String, Integer>> mHosts = new ArrayList<>();

    private AMRMClientAsync<AMRMClient.ContainerRequest> mRMClient = null;
    private NMClientAsync mNMClient = null;

    private NaiadApplicationMaster() throws IOException {
    }

    private Options createAppMasterOptions() {
        Options opts = new Options();
        opts.addOption("help", false, "Print Usage");
        opts.addOption("app_id", true, "Application id. Default to `Unknown`");
        opts.addOption("app_master_log_dir", true, "Log directory where application master stores its logs");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run a worker node");
        opts.addOption("container_vcores", true, "Number of virtual cores to be requested to run a worker node");
        opts.addOption("app_priority", true, "A number to indicate the priority to run a worker node");

        opts.addOption("program", true, "Location of naiad executable");
        opts.addOption("port", true, "A starting port for naiad process. The port value will be increased automatically for the processes in the same host");
        opts.addOption("num_process", true,
            "Number of naiad processes. Either num_process or hosts should be declared");
        opts.addOption("hosts", true,
            "Desired hosts and number of processes of each host. Format(split by comma): host1:num1,host2:num2,host3:num3,... Either num_process or hosts should be declared");
        opts.addOption("num_thread", true, "Number of threads in each naiad process");
        opts.addOption("test", false,
            "If given, containers will be requested and created, naiad program will not run but the command to run the program will be written to stdout.");

        return opts;
    }

    private void printUsage() {
        new HelpFormatter().printHelp("NaiadYarnApplicationMaster", createAppMasterOptions());
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
        CommandLine cliParser = new GnuParser().parse(createAppMasterOptions(), args);

        if (args.length == 0 || cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (!cliParser.hasOption("app_master_log_dir")) {
            throw new IllegalArgumentException("Log directory of application master is not set");
        }
        mAppMasterLogDir = cliParser.getOptionValue("app_master_log_dir");

        mContainerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "2048"));
        if (mContainerMemory < 0) {
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
                "Illegal priority for application. Specified priority: " + mAppPriority);
        }

        mAppId = cliParser.getOptionValue("app_id", mAppId);

        if (!cliParser.hasOption("program")) {
            throw new IllegalArgumentException("The path to naiad program is not given.");
        }
        mProgram = cliParser.getOptionValue("program");

        if (cliParser.hasOption("hosts")) {
            String value = cliParser.getOptionValue("hosts");
            try {
                for (String pair : value.split(",")) {
                    String[] vals = pair.trim().split(":");
                    mHosts.add(new Pair<>(vals[0], Integer.valueOf(vals[1])));
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

    private AMRMClient.ContainerRequest setupContainerAskForRMSpecific() {
        return setupContainerAskForRMSpecific(null);
    }

    private AMRMClient.ContainerRequest setupContainerAskForRMSpecific(String host) {
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(mAppPriority);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(mContainerMemory);
        capability.setVirtualCores(mNumVirtualCores);

        // The second arg controls the hosts of containers
        if (host == null) {
            return new AMRMClient.ContainerRequest(capability, null, null, priority, true);
        } else {
            return new AMRMClient.ContainerRequest(capability, new String[]{host}, null, priority, false);
        }
    }

    private void run() throws YarnException, IOException, InterruptedException, ExecutionException {
        LOG.info("Start App Master, log directory is " + mAppMasterLogDir);

        NaiadRMCallbackHandler mRMClientListener = new NaiadRMCallbackHandler(this);
        mRMClient = AMRMClientAsync.createAMRMClientAsync(1000, mRMClientListener);
        mRMClient.init(mYarnConf);
        mRMClient.start();

        NaiadNMCallbackHandler mContainerListener = new NaiadNMCallbackHandler();
        mNMClient = NMClientAsync.createNMClientAsync(mContainerListener);
        mNMClient.init(mYarnConf);
        mNMClient.start();

        // Register with ResourceManager
        LOG.info("registerApplicationMaster started");
        mRMClient.registerApplicationMaster("", 0, "");
        LOG.info("registerApplicationMaster done");

        // Ask RM to start `mNumContainer` containers, each is a worker node
        LOG.info("Ask RM for " + mNumProcesses + " containers");
        if (mHosts.isEmpty()) {
            for (int i = 0; i < mNumProcesses; i++) {
                mRMClient.addContainerRequest(setupContainerAskForRMSpecific());
            }
        } else {
            for (Pair<String, Integer> pair : mHosts) {
                for (int i = 0; i < pair.getSecond(); i++) {
                    mRMClient.addContainerRequest(setupContainerAskForRMSpecific(pair.getFirst()));
                }
            }
        }

        FinalApplicationStatus status = mRMClientListener.getFinalNumSuccess() == mNumProcesses
            ? FinalApplicationStatus.SUCCEEDED : FinalApplicationStatus.FAILED;

        mRMClient.unregisterApplicationMaster(status, mRMClientListener.getStatusReport(), null);
    }

    int getNumProcesses() {
        return mNumProcesses;
    }

    AMRMClientAsync<AMRMClient.ContainerRequest> getRMClient() {
        return mRMClient;
    }

    NMClientAsync getNMClient() {
        return mNMClient;
    }

    int getPort() {
        return mPort;
    }

    int getNumThreads() {
        return mNumThreads;
    }

    String getProgram() {
        return mProgram;
    }

    String getAppId() {
        return mAppId;
    }

    String getLogDir() {
        return mAppMasterLogDir;
    }

    boolean isTest() {
        return mIsTest;
    }

    static public void main(String[] args) {
        LOG.info("Start running NaiadApplicationMaster");
        try {
            NaiadApplicationMaster appMaster = new NaiadApplicationMaster();
            if (!appMaster.init(args)) {
                System.exit(0);
            }
            appMaster.run();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error running NaiadApplicationMaster", e);
            System.exit(-1);
        }
        LOG.info("NaiadApplicationMaster completed successfully");
        System.exit(0);
    }
}
