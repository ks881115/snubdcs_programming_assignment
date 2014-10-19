package edu.snu.cms.reef.hw1.llr;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelReceiveAckReducer;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;

import javax.inject.Inject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class ComputeTask implements Task {
	private static final Logger LOG = Logger.getLogger(ComputeTask.class
			.getName());

	private final CommunicationGroupClient communicationGroupClient;
	private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
	private final Broadcast.Receiver<DenseVector> thetaBroadcaster;
	private final Reduce.Sender<DenseVector> resultReducer;
	private final Reduce.Sender<Integer> totalCountReducer;
	private final Reduce.Sender<Integer> successCountReducer;

	private final DataSet<LongWritable, Text> dataSet;

	@Inject
	public ComputeTask(final GroupCommClient groupCommClient,
			final DataSet<LongWritable, Text> dataSet) {
		this.communicationGroupClient = groupCommClient
				.getCommunicationGroup(AllCommunicationGroup.class);
		this.controlMessageBroadcaster = communicationGroupClient
				.getBroadcastReceiver(ControlMessageBroadcaster.class);
		this.thetaBroadcaster = communicationGroupClient
				.getBroadcastReceiver(ModelBroadcaster.class);
		this.resultReducer = communicationGroupClient
				.getReduceSender(ReceivedVectorReducer.class);
		this.totalCountReducer = communicationGroupClient
				.getReduceSender(IntegerSumReducer.class);
		this.successCountReducer = communicationGroupClient
				.getReduceSender(IntegerSumReducer.class);

		this.dataSet = dataSet;
	}

	final public double innerProduct(final DenseVector AT, final DenseVector B) {
		double sum = 0.0;
		// Skip the last element since it is y
		for (int i = 0; i < AT.size() - 1; i++) {
			sum += AT.get(i) * B.get(i);
		}
		return sum;
	}

	final public double sigmoid(double x) {
		return 1.0 / (1.0 + Math.exp(-x));
	}

	final public DenseVector computeRow(final DenseVector theta,
			final DenseVector row) {
		DenseVector newRow = new DenseVector(row.size());

		final double hypothesis = sigmoid(innerProduct(theta, row));
		// Skip the last element since it is y
		for (int i = 0; i < row.size() - 1; i++) {
			newRow.set(i, (hypothesis - getY(row)) * row.get(i));
		}

		return newRow;
	}
	
	final public double getY(final DenseVector vector) {
		return vector.get(vector.size() - 1);
	}

	@Override
	public byte[] call(final byte[] memento) throws Exception {
		LOG.log(Level.FINER, "Logistic regression task started");

		System.out.println("Begin data loading");
		
		// Read inputs in advance and avoid redundant read of file
		ArrayList<DenseVector> vectors = new ArrayList<DenseVector>();
		for (final Pair<LongWritable, Text> keyValue : dataSet) {
			String[] tokens = keyValue.second.toString().split(",");
			DenseVector vector = new DenseVector(tokens.length);
			for (int i = 0; i < tokens.length; i++) {
				vector.set(i, Double.parseDouble(tokens[i]));
			}
			vectors.add(vector);
		}
		
		System.out.println("Finished data loading. Read " + vectors.size() + " rows.");
		
		// Reduce # of rows
		totalCountReducer.send(vectors.size());

		// loop until 'Stop' message received
		boolean stop = false;
		while (!stop) {
			// Receive control message
			final ControlMessages controlMessage = controlMessageBroadcaster
					.receive();
			switch (controlMessage) {
			case Stop:
				System.out.println("'Stop' msg received");
				stop = true;
				break;
			case Compute:
				System.out.println("'Compute' msg received");
				DenseVector theta = thetaBroadcaster.receive();

				// Compute partial result of allocated rows
				DenseVector partialSum = new DenseVector(theta.size());
				for (int i = 0; i < vectors.size(); i++) {
					partialSum.add(computeRow(theta, vectors.get(i)));
				}

				resultReducer.send(partialSum);
				System.out.println("Sent computed results");
				break;
			case Evaluate:
				theta = thetaBroadcaster.receive();
				int successCount = 0;
				for(int i = 0; i < vectors.size(); i++) {
					DenseVector row = vectors.get(i);
					// Evaluate with given theta
					double evaluatedResult = (sigmoid(innerProduct(theta, row)) > 0.5) ? 1.0 : 0.0;
					System.out.println("Evaluated result is : " + evaluatedResult);
					// Compare with real result
					successCount += (evaluatedResult == getY(row)) ? 1 : 0;
				}
				successCountReducer.send(successCount);
				
				break;
			default:
				break;
			}
		}
		
		System.out.println("Task finished");

		return memento;
	}
}
