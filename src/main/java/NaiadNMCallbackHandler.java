import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NaiadNMCallbackHandler implements NMClientAsync.CallbackHandler {
    static private final Logger LOG = Logger.getLogger(NaiadNMCallbackHandler.class.getName());

    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
        LOG.info("Container with id " + containerId + " starts.");
    }

    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    public void onContainerStopped(ContainerId containerId) {
        LOG.info("Container with id " + containerId + " is stopped.");
    }

    public void onStartContainerError(ContainerId containerId, Throwable throwable) {
        LOG.log(Level.SEVERE, "Error thrown while starting container with id " + containerId, throwable);
    }

    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
        LOG.log(Level.WARNING, "Error thrown while fetching status of container with id " + containerId, throwable);
    }

    public void onStopContainerError(ContainerId containerId, Throwable throwable) {
        LOG.log(Level.WARNING, "Error thrown while stopping container with id " + containerId, throwable);
    }
}
