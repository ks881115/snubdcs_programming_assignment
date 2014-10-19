/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cms.reef.hw1.llr;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelReceiveAckReducer;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.mortbay.log.Log;

import javax.inject.Inject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The task that iterates over the data set to count the number of records.
 * Assumes TextInputFormat and that records represent lines.
 */
@TaskSide
public class ControllerTask implements Task {

	public static final String TASK_ID = "ControllerTask";

	private static final Logger LOG = Logger.getLogger(ControllerTask.class
			.getName());

	private final CommunicationGroupClient communicationGroupClient;
	private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
	private final Broadcast.Sender<DenseVector> thetaBroadcaster;
	private final Reduce.Receiver<DenseVector> modelReceiveAckReducer;
	private final Reduce.Receiver<Integer> totalCountReducer;
	private final Reduce.Receiver<Integer> successCountReducer;

	private final int dimensions;

	@Inject
	public ControllerTask(final GroupCommClient groupCommClient,
			final @Parameter(ModelDimensions.class) int dimensions) {

		this.dimensions = dimensions;

		this.communicationGroupClient = groupCommClient
				.getCommunicationGroup(AllCommunicationGroup.class);
		this.controlMessageBroadcaster = communicationGroupClient
				.getBroadcastSender(ControlMessageBroadcaster.class);
		this.thetaBroadcaster = communicationGroupClient
				.getBroadcastSender(ModelBroadcaster.class);
		this.modelReceiveAckReducer = communicationGroupClient
				.getReduceReceiver(ReceivedVectorReducer.class);
		this.totalCountReducer = communicationGroupClient
				.getReduceReceiver(IntegerSumReducer.class);
		this.successCountReducer = communicationGroupClient
				.getReduceReceiver(IntegerSumReducer.class);
	}

	// No convergence check
	private boolean isConverged(DenseVector oldTheta, DenseVector newTheta) {
		return false;
	}

	private void initializeTheta(DenseVector theta, double val) {
		// Skip the last element since it is y
		for (int i = 0; i < theta.size() - 1; i++) {
			theta.set(i, val);
		}
	}

	private DenseVector recomputeTheta(DenseVector oldTheta,
			DenseVector computedResults) {
		final double ALPHA = 0.1;

		DenseVector newTheta = new DenseVector(oldTheta.size());
		// Skip the first element since it is y
		for (int i = 0; i < oldTheta.size() - 1; i++) {
			newTheta.set(i, oldTheta.get(i) - ALPHA * computedResults.get(i));
		}

		return newTheta;
	}

	@Override
	public byte[] call(final byte[] memento) throws Exception {
		DenseVector theta = new DenseVector(dimensions), oldTheta = null, computedResults = null;
		final int maxIters = 500;

		// Initialize theta with default value
		initializeTheta(theta, 1.0);
		
		int totalCount = totalCountReducer.reduce();

		int i = 0;
		do {
			controlMessageBroadcaster.send(ControlMessages.Compute);
			// Send theta to every compute node
			thetaBroadcaster.send(theta);
			// Receive reduced result
			computedResults = modelReceiveAckReducer.reduce();

			oldTheta = theta;
			// Recompute theta
			theta = recomputeTheta(oldTheta, computedResults);
			
			// Evaluate computed theta
			controlMessageBroadcaster.send(ControlMessages.Evaluate);
			thetaBroadcaster.send(theta);
			int successCount = successCountReducer.reduce();
			
			// Print success rate of the evaluated parameters(=theta)
			System.out.println("Success rate is: " + successCount + " / " + totalCount + " = " + (double) successCount / (double) totalCount);
			
			final GroupChanges changes = communicationGroupClient
					.getTopologyChanges();
			if (changes.exist()) {
				Log.info("There exist topology changes. Asking to update Topology");
				communicationGroupClient.updateTopology();
			} else {
				Log.info("No changes in topology exist. So not updating topology");
			}
		} while (i++ < maxIters && !isConverged(oldTheta, theta));
		
		// Stop compute tasks
		controlMessageBroadcaster.send(ControlMessages.Stop);
		
		return null;
	}
}