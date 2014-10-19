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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.broadcast.MasterTask;
import com.microsoft.reef.examples.nggroup.broadcast.ModelReceiveAckReduceFunction;
import com.microsoft.reef.examples.nggroup.broadcast.SlaveTask;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.ModelReceiveAckReducer;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.NumberOfReceivers;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;

/**
 * Driver side for the line counting demo that uses the data loading service.
 */
@DriverSide
@Unit
public class LinearLogisticRegressionDriver {

	private static final Logger LOG = Logger
			.getLogger(LinearLogisticRegressionDriver.class.getName());

	private final AtomicInteger ctrlCtxIds = new AtomicInteger();
	private final AtomicInteger lineCnt = new AtomicInteger();
	private final AtomicInteger completedDataTasks = new AtomicInteger();

	private final DataLoadingService dataLoadingService;
	private final GroupCommDriver groupCommDriver;
	private final CommunicationGroupDriver allCommGroup;

	private String groupCommConfiguredMasterId;

	/*
	 * private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);
	 * private final AtomicInteger slaveIds = new AtomicInteger(0); private
	 * final AtomicInteger failureSet = new AtomicInteger(0);
	 * 
	 * private final GroupCommDriver groupCommDriver; private final
	 * CommunicationGroupDriver allCommGroup; private final
	 * ConfigurationSerializer confSerializer; private final int dimensions;
	 * private final EvaluatorRequestor requestor; private final int
	 * numberOfReceivers; private final AtomicInteger
	 * numberOfAllocatedEvaluators;
	 */
	private final int dimensions;
	private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);
	private final int numOfController = 1;

	@Inject
	public LinearLogisticRegressionDriver(
			final DataLoadingService dataLoadingService,
			final GroupCommDriver groupCommDriver,
			final @Parameter(NumberOfReceivers.class) int numberOfReceivers,
			final @Parameter(ModelDimensions.class) int dimensions) {
		this.dataLoadingService = dataLoadingService;
		this.groupCommDriver = groupCommDriver;
		this.completedDataTasks.set(dataLoadingService.getNumberOfPartitions());
		this.dimensions = dimensions;

		this.allCommGroup = this.groupCommDriver.newCommunicationGroup(
				AllCommunicationGroup.class, numberOfReceivers
						+ numOfController);

		LOG.info("Obtained all communication group");

		this.allCommGroup
				.addBroadcast(
						ControlMessageBroadcaster.class,
						BroadcastOperatorSpec.newBuilder()
								.setSenderId(ControllerTask.TASK_ID)
								.setDataCodecClass(SerializableCodec.class)
								.build())
				.addBroadcast(
						ModelBroadcaster.class,
						BroadcastOperatorSpec.newBuilder()
								.setSenderId(ControllerTask.TASK_ID)
								.setDataCodecClass(SerializableCodec.class)
								.build())
				.addReduce(
						ReceivedVectorReducer.class,
						ReduceOperatorSpec
								.newBuilder()
								.setReceiverId(ControllerTask.TASK_ID)
								.setDataCodecClass(SerializableCodec.class)
								.setReduceFunctionClass(
										ReceivedVectorReduceFunction.class)
								.build())
				.addReduce(
						IntegerSumReducer.class,
						ReduceOperatorSpec
								.newBuilder()
								.setReceiverId(ControllerTask.TASK_ID)
								.setDataCodecClass(SerializableCodec.class)
								.setReduceFunctionClass(
										IntegerSumReduceFunction.class).build())
				.addReduce(
						IntegerSumReducer.class,
						ReduceOperatorSpec
								.newBuilder()
								.setReceiverId(ControllerTask.TASK_ID)
								.setDataCodecClass(SerializableCodec.class)
								.setReduceFunctionClass(
										IntegerSumReduceFunction.class).build())
				.finalise();
	}

	public class ContextActiveHandler implements EventHandler<ActiveContext> {

		private final AtomicBoolean storeMasterId = new AtomicBoolean(false);

		@Override
		public void onNext(final ActiveContext activeContext) {
			if (groupCommDriver.isConfigured(activeContext)) {
				final String contextId = activeContext.getId();
				LOG.log(Level.FINER, "Context active: {0}", contextId);

				if (activeContext.getId().equals(groupCommConfiguredMasterId)
						&& !masterTaskSubmitted()) {
					final Configuration partialTaskConf = Tang.Factory
							.getTang()
							.newConfigurationBuilder(
									TaskConfiguration.CONF
											.set(TaskConfiguration.IDENTIFIER,
													ControllerTask.TASK_ID)
											.set(TaskConfiguration.TASK,
													ControllerTask.class)
											.build())
							.bindNamedParameter(ModelDimensions.class,
									Integer.toString(dimensions)).build();

					allCommGroup.addTask(partialTaskConf);

					final Configuration taskConf = groupCommDriver
							.getTaskConfiguration(partialTaskConf);

					activeContext.submitTask(taskConf);
					System.out.println("Submitted controller task");
				} else {
					final String taskId = "ComputeTask-"
							+ ctrlCtxIds.getAndIncrement();

					final Configuration partialTaskConf = Tang.Factory
							.getTang()
							.newConfigurationBuilder(
									TaskConfiguration.CONF
											// .set(TaskConfiguration.IDENTIFIER,
											// activeContext.getId())
											// .set(TaskConfiguration.TASK,
											// SlaveTask.class)
											.set(TaskConfiguration.IDENTIFIER,
													taskId)
											.set(TaskConfiguration.TASK,
													ComputeTask.class).build())
							.bindNamedParameter(ModelDimensions.class,
									Integer.toString(dimensions)).build();

					allCommGroup.addTask(partialTaskConf);
					final Configuration taskConf = groupCommDriver
							.getTaskConfiguration(partialTaskConf);

					try {
						activeContext.submitTask(taskConf);
					} catch (final BindException ex) {
						LOG.log(Level.SEVERE, "Configuration error in "
								+ contextId, ex);
						throw new RuntimeException("Configuration error in "
								+ contextId, ex);
					}
					System.out.println("Submitted compute task");
				}
			} else {

				Configuration contextConf = groupCommDriver
						.getContextConfiguration();
				final String contextId = contextId(contextConf);

				if (storeMasterId.compareAndSet(false, true)) {
					groupCommConfiguredMasterId = contextId;
				}

				final Configuration serviceConf = groupCommDriver
						.getServiceConfiguration();

				activeContext.submitContextAndService(contextConf, serviceConf);
				System.out.println("Submitted Group comm service");
			}

		}

		private String contextId(final Configuration contextConf) {
			try {
				final Injector injector = Tang.Factory.getTang().newInjector(
						contextConf);
				return injector.getNamedInstance(ContextIdentifier.class);
			} catch (final InjectionException e) {
				throw new RuntimeException(
						"Unable to inject context identifier from context conf",
						e);
			}
		}

		private boolean masterTaskSubmitted() {
			return !masterSubmitted.compareAndSet(false, true);
		}
	}

	public class FailedTaskHandler implements EventHandler<FailedTask> {

		@Override
		public void onNext(final FailedTask failedTask) {

			LOG.log(Level.FINE, "Got failed Task: {0}", failedTask.getId());

			final ActiveContext activeContext = failedTask.getActiveContext()
					.get();
			final Configuration partialTaskConf = Tang.Factory
					.getTang()
					.newConfigurationBuilder(
							TaskConfiguration.CONF
									.set(TaskConfiguration.IDENTIFIER,
											failedTask.getId())
									.set(TaskConfiguration.TASK,
											ComputeTask.class).build())
					.bindNamedParameter(ModelDimensions.class, "" + dimensions)
					.build();

			final Configuration taskConf = groupCommDriver
					.getTaskConfiguration(partialTaskConf);
			
			activeContext.submitTask(taskConf);
		}
	}
}