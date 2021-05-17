/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.workers.temporal;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.io.IOs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.WorkerUtils;
import io.temporal.activity.Activity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class allow a worker to run multiple times. In addition to the functionality in
 * TemporalAttemptExecution it takes a predicate to determine if the output of a worker constitutes
 * a complete success or a partial one. It also takes a function that takes in the input of the
 * previous run of the worker and the output of the last worker in order to generate a new input for
 * that worker.
 */
public class PartialSuccessTemporalAttemptExecution<INPUT, OUTPUT> implements Supplier<List<OUTPUT>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartialSuccessTemporalAttemptExecution.class);

  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
  public static String WORKFLOW_ID_FILENAME = "WORKFLOW_ID";

  private final Path jobRoot;
  private final CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier;
  private final Supplier<INPUT> inputSupplier;
  private final String jobId;
  private final BiConsumer<Path, String> mdcSetter;
  private final CheckedConsumer<Path, IOException> jobRootDirCreator;
  private final CancellationHandler cancellationHandler;
  private final Supplier<String> workflowIdProvider;

  private final Predicate<OUTPUT> shouldAttemptAgainPredicate;
  private final BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction;
  private final int maxRetriesCount;

  public PartialSuccessTemporalAttemptExecution(Path workspaceRoot,
                                                JobRunConfig jobRunConfig,
                                                CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                                Supplier<INPUT> initialInputSupplier,
                                                CancellationHandler cancellationHandler,
                                                Predicate<OUTPUT> shouldAttemptAgainPredicate,
                                                BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction,
                                                int maxRetriesCount) {
    this(
        workspaceRoot,
        jobRunConfig,
        workerSupplier,
        initialInputSupplier,
        WorkerUtils::setJobMdc,
        Files::createDirectories,
        cancellationHandler,
        () -> Activity.getExecutionContext().getInfo().getWorkflowId(),
        shouldAttemptAgainPredicate,
        computeNextAttemptInputFunction,
        maxRetriesCount);
  }

  @VisibleForTesting
  PartialSuccessTemporalAttemptExecution(Path workspaceRoot,
                                         JobRunConfig jobRunConfig,
                                         CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                         Supplier<INPUT> initialInputSupplier,
                                         BiConsumer<Path, String> mdcSetter,
                                         CheckedConsumer<Path, IOException> jobRootDirCreator,
                                         CancellationHandler cancellationHandler,
                                         Supplier<String> workflowIdProvider,
                                         Predicate<OUTPUT> shouldAttemptAgainPredicate,
                                         BiFunction<INPUT, OUTPUT, INPUT> computeNextAttemptInputFunction,
                                         int maxRetriesCount) {
    this.jobRoot = WorkerUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    this.workerSupplier = workerSupplier;
    this.inputSupplier = initialInputSupplier;
    this.jobId = jobRunConfig.getJobId();
    this.mdcSetter = mdcSetter;
    this.jobRootDirCreator = jobRootDirCreator;
    this.cancellationHandler = cancellationHandler;
    this.workflowIdProvider = workflowIdProvider;
    this.shouldAttemptAgainPredicate = shouldAttemptAgainPredicate;
    this.computeNextAttemptInputFunction = computeNextAttemptInputFunction;
    this.maxRetriesCount = maxRetriesCount;
  }

  @Override
  public List<OUTPUT> get() {
    try {
      mdcSetter.accept(jobRoot, jobId);

      LOGGER.info("Executing worker wrapper. Airbyte version: {}", new EnvConfigs().getAirbyteVersionOrWarning());
      jobRootDirCreator.accept(jobRoot);

      final String workflowId = workflowIdProvider.get();
      final Path workflowIdFile = jobRoot.getParent().resolve(WORKFLOW_ID_FILENAME);
      IOs.writeFile(workflowIdFile, workflowId);

      final Worker<INPUT, OUTPUT> worker = workerSupplier.get();
      final CompletableFuture<List<OUTPUT>> outputFuture = new CompletableFuture<>();
      final Thread workerThread = getWorkerThread(worker, outputFuture);
      final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      final Runnable cancellationChecker = getCancellationChecker(worker, workerThread, outputFuture);

      // check once first that we are not already cancelled. if we are, don't start!
      cancellationChecker.run();

      workerThread.start();
      scheduledExecutor.scheduleAtFixedRate(cancellationChecker, 0, HEARTBEAT_INTERVAL.toSeconds(), TimeUnit.SECONDS);

      try {
        // block and wait for the output
        return outputFuture.get();
      } finally {
        LOGGER.info("Stopping cancellation check scheduling...");
        scheduledExecutor.shutdown();
      }
    } catch (Exception e) {
      throw Activity.wrap(e);
    }
  }

  private Thread getWorkerThread(Worker<INPUT, OUTPUT> worker, CompletableFuture<List<OUTPUT>> outputFuture) {
    return new Thread(() -> {
      mdcSetter.accept(jobRoot, jobId);
      try {
        INPUT input = inputSupplier.get();
        OUTPUT lastOutput = null;
        List<OUTPUT> outputCollector = new ArrayList<>();

        // if there are retries left and the worker has never run or based on previous out it should run
        // again keep trying.
        int i = 0;
        while (true) {
          if (i >= maxRetriesCount) {
            LOGGER.info("Max retries reached: {}", i);
            break;
          }

          final boolean hasLastOutput = lastOutput != null;
          final boolean shouldAttemptAgain = shouldAttemptAgainPredicate.test(lastOutput);
          LOGGER.info("Last output present: {}. Should attempt again: {}", lastOutput != null, shouldAttemptAgain);
          if (hasLastOutput && !shouldAttemptAgain) {
            break;
          }

          LOGGER.info("Starting attempt: {} of {}", i, maxRetriesCount);

          if (lastOutput != null) {
            input = computeNextAttemptInputFunction.apply(input, lastOutput);
          }
          // each worker run should run in a separate directory.
          final Path attemptRoot = Files.createDirectories(jobRoot.resolve(String.valueOf(i)));
          lastOutput = worker.run(input, attemptRoot);
          outputCollector.add(lastOutput);
          i++;
        }
        outputFuture.complete(outputCollector);
      } catch (Throwable e) {
        LOGGER.info("Completing future exceptionally...", e);
        outputFuture.completeExceptionally(e);
      }
    });
  }

  private Runnable getCancellationChecker(Worker<INPUT, OUTPUT> worker, Thread workerThread, CompletableFuture<List<OUTPUT>> outputFuture) {
    return () -> {
      try {
        mdcSetter.accept(jobRoot, jobId);

        final Runnable onCancellationCallback = () -> {
          LOGGER.info("Running sync worker cancellation...");
          worker.cancel();

          LOGGER.info("Interrupting worker thread...");
          workerThread.interrupt();

          LOGGER.info("Cancelling completable future...");
          outputFuture.cancel(false);
        };

        cancellationHandler.checkAndHandleCancellation(onCancellationCallback);
      } catch (WorkerException e) {
        LOGGER.error("Cancellation checker exception", e);
      }
    };
  }

}
