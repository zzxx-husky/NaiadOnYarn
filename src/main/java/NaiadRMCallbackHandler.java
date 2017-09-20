import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.Records;
import org.mortbay.util.SingletonList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NaiadRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private static final Logger LOG = Logger.getLogger(NaiadRMCallbackHandler.class.getName());

    private NaiadApplicationMaster mAppMaster = null;
    private final Lock finalResultLock = new Lock();
    private final Lock cmdLock = new Lock();

    private int mNumCompletedContainers = 0;
    private int mNumSuccessContainers = 0;

    private ArrayList<Thread> mContainerThreads = new ArrayList<Thread>();
    private HashMap<String, Integer> mHostCmdCounter = new HashMap<>();

    private String mCommandTemplate = "";

    public NaiadRMCallbackHandler(NaiadApplicationMaster appMaster) {
        mAppMaster = appMaster;
    }

    public int getFinalNumSuccess() throws InterruptedException {
        if (finalResultLock.isLocked()) {
            synchronized (finalResultLock) {
                if (finalResultLock.isLocked()) {
                    finalResultLock.wait();
                }
            }
        }
        return mNumSuccessContainers;
    }

    public String getStatusReport() {
        return String.format("Requested: %d, Allocated: %d, Completed: %d, Succeeded: %d, Failed: %d\n", mAppMaster.getNumProcesses(), mContainerThreads.size(),
            mNumCompletedContainers, mNumSuccessContainers, mNumCompletedContainers - mNumSuccessContainers);
    }

    public void onContainersCompleted(List<ContainerStatus> list) {
        LOG.info("Get response from RM for container request, completedCnt = " + list.size());
        mNumCompletedContainers += list.size();
        for (ContainerStatus status : list) {
            LOG.info(String.format("Container %s: %s, exit status: %d", status.getContainerId().toString(),
                status.getState().toString(), status.getExitStatus()));
            if (status.getExitStatus() == 0) {
                mNumSuccessContainers += 1;
            }
        }
        LOG.info("Total containers: " + mAppMaster.getNumProcesses() + ", completed containers: " + mNumCompletedContainers);
        if (mAppMaster.getNumProcesses() == mNumCompletedContainers) {
            // If all workers and master finish
            synchronized (finalResultLock) {
                finalResultLock.unlock();
                finalResultLock.notifyAll();
            }
        }
    }

    String nextCommand(String host) throws Exception {
        if (cmdLock.isLocked()) {
            synchronized (cmdLock) {
                if (cmdLock.isLocked()) {
                    cmdLock.wait();
                }
            }
        }
        int id;
        synchronized (mHostCmdCounter) {
            id = mHostCmdCounter.get(host);
            mHostCmdCounter.put(host, id + 1);
        }
        return String.format(mCommandTemplate, id, id, id);
    }

    public void onContainersAllocated(List<Container> list) {
        LOG.info("Get response from RM for container request, allocatedCnt = " + list.size());
        for (final Container container : list) {
            String host = container.getNodeId().getHost();
            mHostCmdCounter.put(host, (mHostCmdCounter.containsKey(host) ? mHostCmdCounter.get(host) : 0) + 1);
            Thread worker = new Thread() {
                public void run() {
                    String host = container.getNodeId().getHost();
                    LOG.info("New container " + container.getId() + " starts on " + host);
                    try {
                        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                        ctx.setCommands(SingletonList.newSingletonList(nextCommand(host)));
                        mAppMaster.getNMClient().startContainerAsync(container, ctx);
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "Failed to start worker container on " + host, e);
                    }
                }
            };
            worker.start();
            mContainerThreads.add(worker);
        }
        if (mContainerThreads.size() == mAppMaster.getNumProcesses()) {
            StringBuilder builder = new StringBuilder();
            if (mAppMaster.isTest()) {
                builder.append("echo 'This is $(hostname): ");
            }
            builder.append(mAppMaster.getProgram())
                .append(" -t ").append(mAppMaster.getNumThreads())
                .append(" -n ").append(mAppMaster.getNumProcesses())
                .append(" -p %d").append(" -h"); // %d will be formated in nextCommand method.
            int numProc = 0;
            for (String key : mHostCmdCounter.keySet()) {
                int val = mHostCmdCounter.get(key);
                mHostCmdCounter.put(key, numProc);
                numProc += val;
                for (int i = 0; i < val; i++) {
                    builder.append(' ').append(key).append(':').append(i + mAppMaster.getPort());
                }
            }
            if (mAppMaster.isTest()) {
                builder.append('\'');
            }
            builder.append(" 1>").append(mAppMaster.getLogDir()).append('/').append(mAppMaster.getAppId()).append("-container-%d.out");
            builder.append(" 2>").append(mAppMaster.getLogDir()).append('/').append(mAppMaster.getAppId()).append("-container-%d.err");
            mCommandTemplate = builder.toString();

            LOG.info("Containers are ready. Command template is `" + mCommandTemplate + "`. Start to launch naiad processes.");
            synchronized (cmdLock) {
                cmdLock.unlock();
                cmdLock.notifyAll();
            }
        }
    }

    public void onShutdownRequest() {
    }

    public void onNodesUpdated(List<NodeReport> list) {
    }

    public float getProgress() {
        return ((float) mNumCompletedContainers) / mAppMaster.getNumProcesses();
    }

    public void onError(Throwable throwable) {
        mAppMaster.getRMClient().stop();
    }
}
