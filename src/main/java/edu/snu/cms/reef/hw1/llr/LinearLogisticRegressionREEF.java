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

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.broadcast.parameters.NumberOfReceivers;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the data loading demo app
 */
@ClientSide
public class LinearLogisticRegressionREEF {

  private static final Logger LOG = Logger.getLogger(LinearLogisticRegressionREEF.class.getName());

  private static final int NUM_LOCAL_THREADS = 3;
  private static final int NUM_SPLITS = 2;
  private static final int NUM_COMPUTE_EVALUATORS = 2;

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "10")
  public static final class TimeOut implements Name<Integer> {
  }

  // Give file path here
  @NamedParameter(short_name = "input", default_value = "file:/Users/lee/workspace/shimoga/spambase.data.txt")
  public static final class InputDir implements Name<String> {
  }

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(Local.class)
        .registerShortNameOfClass(TimeOut.class)
        .registerShortNameOfClass(LinearLogisticRegressionREEF.InputDir.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(LinearLogisticRegressionREEF.InputDir.class);

    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running Data Loading demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(NUM_COMPUTE_EVALUATORS)
        .setMemory(512)
        .setNumber(1)
        .build();

    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(1024)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(NUM_SPLITS)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(LinearLogisticRegressionDriver.class))
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, LinearLogisticRegressionDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, LinearLogisticRegressionDriver.FailedTaskHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "LinearLogisticRegressionDriver"))
        .build();

    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    // Temporarily use this
    int numberOfReceivers = NUM_SPLITS;
    // # of dimension of the dataset used
    int dimensions = 58;
    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(dataLoadConfiguration, groupCommServConfiguration)
        .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
        .bindNamedParameter(NumberOfReceivers.class, Integer.toString(numberOfReceivers))
        .build();

    final LauncherStatus state =
    	DriverLauncher.getLauncher(runtimeConfiguration).run(mergedDriverConfiguration, jobTimeout);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }
}