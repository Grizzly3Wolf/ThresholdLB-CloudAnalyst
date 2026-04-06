package cloudsim.ext.datacenter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cloudsim.ext.Constants;
import cloudsim.ext.event.CloudSimEvent;
import cloudsim.ext.event.CloudSimEventListener;
import cloudsim.ext.event.CloudSimEvents;

/**
 * ThresholdLB (Threshold Based Load Balancer) distributes incoming tasks among
 * Virtual Machines using two threshold values:
 *
 *  - OVERLOAD_THRESHOLD : maximum number of concurrent tasks a VM may hold
 *                         before it is considered overloaded.
 *  - UNDERLOAD_THRESHOLD: once a previously-overloaded VM's active task count
 *                         falls to or below this value it is moved back to the
 *                         underloaded pool and becomes eligible for new tasks.
 *
 * All VMs start in the underloaded pool. When a task arrives the balancer picks
 * a VM from that pool whose task count is still within the overload threshold.
 * If a VM has reached the threshold it is moved to the overloaded pool. When all
 * VMs are overloaded the task is handed to the least-loaded VM among the
 * overloaded pool (fallback, matching the flowchart). Task completions are
 * tracked via CloudSim events so that overloaded VMs can graduate back to the
 * underloaded pool when their load drops sufficiently.
 *
 * Implements Algorithm 1 (Task Scheduling) and Algorithm 2 (Task Completion)
 * from the Threshold Based Load Balancing paper.
 *
 * @author  [Your Name]
 */
public class ThresholdLB extends VmLoadBalancer implements CloudSimEventListener {

    // -----------------------------------------------------------------------
    // Configurable thresholds
    // -----------------------------------------------------------------------

    /** A VM whose active task count EXCEEDS this value is treated as overloaded. */
    private static final int OVERLOAD_THRESHOLD  = 75;

    /**
     * An overloaded VM whose active task count drops to or BELOW this value is
     * moved back to the underloaded pool.
     */
    private static final int UNDERLOAD_THRESHOLD = 25;

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Maps vmId → current number of active (allocated but not yet finished) tasks. */
    private final Map<Integer, Integer> currentAllocationCounts;

    /** Reference to the datacenter's live VM-state map (populated as VMs start). */
    private final Map<Integer, VirtualMachineState> vmStatesList;

    /**
     * Pool of VMs whose active task count is at or below OVERLOAD_THRESHOLD.
     * All VMs start here (Algorithm 1, line 1).
     */
    private final List<Integer> underloadVMs;

    /**
     * Pool of VMs that have exceeded OVERLOAD_THRESHOLD.
     * Initially empty (Algorithm 1, line 2).
     */
    private final List<Integer> overloadVMs;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    public ThresholdLB(DatacenterController dcb) {
        dcb.addCloudSimEventListener(this);

        this.vmStatesList            = dcb.getVmStatesList();
        this.currentAllocationCounts = Collections.synchronizedMap(new HashMap<>());
        this.underloadVMs            = Collections.synchronizedList(new ArrayList<>());
        this.overloadVMs             = Collections.synchronizedList(new ArrayList<>());

        // Initialise every known VM as underloaded with zero active tasks.
        // (Algorithm 1, line 1: underLoadVMs ← [VM1, VM2, … VMn])
        for (int vmId : vmStatesList.keySet()) {
            underloadVMs.add(vmId);
            currentAllocationCounts.put(vmId, 0);
        }
    }

    // -----------------------------------------------------------------------
    // Algorithm 1 – Task Scheduling (getNextAvailableVm)
    // -----------------------------------------------------------------------

    /**
     * Returns the id of the VM to which the next incoming task should be sent.
     *
     * Procedure (Algorithm 1):
     *   1. Ensure the underloaded pool reflects the current VM inventory.
     *   2. Scan the underloaded pool; any VM whose count already exceeds the
     *      overload threshold is moved to the overloaded pool on-the-fly.
     *   3. The first VM that is still within the threshold receives the task.
     *   4. If every VM is overloaded, fall back to the VM with the fewest
     *      active tasks among the overloaded pool (flowchart fallback path).
     */
    @Override
    public int getNextAvailableVm() {

        // Lazily register any VMs that started after construction.
        syncNewVmsToUnderloadPool();

        int selectedVmId = -1;

        // --- Algorithm 1, lines 5-13: scan underloaded pool ---
        List<Integer> nowOverloaded = new ArrayList<>();

        for (int candidateVmId : new ArrayList<>(underloadVMs)) {
            int taskCount = getTaskCount(candidateVmId);

            if (taskCount <= OVERLOAD_THRESHOLD) {
                // This VM can still accept a task (Algorithm 1, line 7).
                selectedVmId = candidateVmId;
                break;
            } else {
                // VM has reached its limit – move to overloaded pool
                // (Algorithm 1, lines 10-11).
                nowOverloaded.add(candidateVmId);
            }
        }

        // Apply deferred pool migrations.
        for (int ovId : nowOverloaded) {
            underloadVMs.remove(Integer.valueOf(ovId));
            if (!overloadVMs.contains(ovId)) {
                overloadVMs.add(ovId);
            }
        }

        // --- Fallback: all VMs overloaded (flowchart "If all VMs are overloaded") ---
        if (selectedVmId == -1) {
            selectedVmId = getLeastLoadedVmFrom(overloadVMs);
        }

        if (selectedVmId != -1) {
            allocatedVm(selectedVmId);
        }

        return selectedVmId;
    }

    // -----------------------------------------------------------------------
    // Algorithm 2 – Task Completion (cloudSimEventFired)
    // -----------------------------------------------------------------------

    /**
     * Reacts to CloudSim events:
     *
     *  EVENT_CLOUDLET_ALLOCATED_TO_VM  – a task has been dispatched; increment
     *                                    the VM's active-task counter and check
     *                                    whether the VM crosses the overload
     *                                    threshold (Algorithm 1, line 9).
     *
     *  EVENT_VM_FINISHED_CLOUDLET      – a task has completed; decrement the
     *                                    counter and, if the VM is in the
     *                                    overloaded pool and has now dropped to
     *                                    or below UNDERLOAD_THRESHOLD, move it
     *                                    back to the underloaded pool
     *                                    (Algorithm 2, lines 1-8).
     */
    @Override
    public void cloudSimEventFired(CloudSimEvent e) {

        if (e.getId() == CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM) {

            // ----------------------------------------------------------------
            // Algorithm 1, line 9 – increment task count after allocation
            // ----------------------------------------------------------------
            int vmId     = (Integer) e.getParameter(Constants.PARAM_VM_ID);
            int newCount = getTaskCount(vmId) + 1;
            currentAllocationCounts.put(vmId, newCount);

            // If the VM just crossed the overload threshold, migrate it.
            if (newCount > OVERLOAD_THRESHOLD) {
                if (underloadVMs.remove(Integer.valueOf(vmId))) {
                    if (!overloadVMs.contains(vmId)) {
                        overloadVMs.add(vmId);
                    }
                }
            }

        } else if (e.getId() == CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET) {

            // ----------------------------------------------------------------
            // Algorithm 2 – task completion handler
            // Algorithm 2, line 1: allocatedVM ← completedTask.VM
            // Algorithm 2, line 2: allocatedVM.taskCount ← taskCount – 1
            // ----------------------------------------------------------------
            int vmId     = (Integer) e.getParameter(Constants.PARAM_VM_ID);
            int newCount = Math.max(0, getTaskCount(vmId) - 1);   // guard against underflow
            currentAllocationCounts.put(vmId, newCount);

            // Algorithm 2, lines 3-7:
            // if overloadVMs contains allocatedVM AND taskCount ≤ underloadThreshold
            //     move from overloaded → underloaded pool
            // else
            //     update underloaded pool entry (count already updated above)
            if (overloadVMs.contains(vmId) && newCount <= UNDERLOAD_THRESHOLD) {
                // Algorithm 2, lines 4-5
                overloadVMs.remove(Integer.valueOf(vmId));
                if (!underloadVMs.contains(vmId)) {
                    underloadVMs.add(vmId);
                }
            }
            // else – VM is already in the underloaded pool; count updated in-place (Algorithm 2, line 7).
        }
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    /**
     * Returns the current active-task count for the given VM id.
     * Returns 0 if the VM has not yet been seen.
     */
    private int getTaskCount(int vmId) {
        Integer count = currentAllocationCounts.get(vmId);
        return (count == null) ? 0 : count;
    }

    /**
     * Scans the global VM-state list for VMs that appeared after construction
     * and adds them to the underloaded pool with an initial count of zero.
     */
    private void syncNewVmsToUnderloadPool() {
        for (int vmId : vmStatesList.keySet()) {
            if (!currentAllocationCounts.containsKey(vmId)) {
                currentAllocationCounts.put(vmId, 0);
                underloadVMs.add(vmId);
            }
        }
    }

    /**
     * Returns the id of the VM with the fewest active tasks from the given pool,
     * or -1 if the pool is empty.
     */
    private int getLeastLoadedVmFrom(List<Integer> pool) {
        int selectedId = -1;
        int minCount   = Integer.MAX_VALUE;

        for (int vmId : pool) {
            int count = getTaskCount(vmId);
            if (count < minCount) {
                minCount   = count;
                selectedId = vmId;
            }
        }
        return selectedId;
    }

    // -----------------------------------------------------------------------
    // Accessors (useful for monitoring / testing)
    // -----------------------------------------------------------------------

    /** Returns a read-only snapshot of the current underloaded VM pool. */
    public List<Integer> getUnderloadVMs() {
        return Collections.unmodifiableList(new ArrayList<>(underloadVMs));
    }

    /** Returns a read-only snapshot of the current overloaded VM pool. */
    public List<Integer> getOverloadVMs() {
        return Collections.unmodifiableList(new ArrayList<>(overloadVMs));
    }

    /** Returns the active-task count for a specific VM (for external monitoring). */
    public int getActiveTaskCount(int vmId) {
        return getTaskCount(vmId);
    }

    /** Returns the configured overload threshold. */
    public int getOverloadThreshold() {
        return OVERLOAD_THRESHOLD;
    }

    /** Returns the configured underload threshold. */
    public int getUnderloadThreshold() {
        return UNDERLOAD_THRESHOLD;
    }
}