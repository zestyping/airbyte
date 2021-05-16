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
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.WorkerUtils;
import io.temporal.activity.Activity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
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
public class PartialSuccessTemporalAttemptExecution<INPUT, OUTPUT> extends TemporalAttemptExecution<INPUT, OUTPUT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartialSuccessTemporalAttemptExecution.class);

  private final Predicate<OUTPUT> succeeded;
  private final BiFunction<INPUT, OUTPUT, INPUT> nextInput;
  private final int maxRetriesCount;

  public PartialSuccessTemporalAttemptExecution(Path workspaceRoot,
                                                JobRunConfig jobRunConfig,
                                                CheckedSupplier<Worker<INPUT, OUTPUT>, Exception> workerSupplier,
                                                Supplier<INPUT> initialInputSupplier,
                                                CancellationHandler cancellationHandler,
                                                Predicate<OUTPUT> succeeded,
                                                BiFunction<INPUT, OUTPUT, INPUT> nextInput,
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
        succeeded,
        nextInput,
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
                                         Predicate<OUTPUT> succeeded,
                                         BiFunction<INPUT, OUTPUT, INPUT> nextInput,
                                         int maxRetriesCount) {
    super(
        workspaceRoot,
        jobRunConfig,
        workerSupplier,
        initialInputSupplier,
        mdcSetter,
        jobRootDirCreator,
        cancellationHandler,
        workflowIdProvider);

    this.succeeded = succeeded;
    this.nextInput = nextInput;
    this.maxRetriesCount = maxRetriesCount;
  }

  @Override
  protected Thread getWorkerThread(Worker<INPUT, OUTPUT> worker, CompletableFuture<OUTPUT> outputFuture) {
    return new Thread(() -> {
      mdcSetter.accept(jobRoot, jobId);
      try {
        INPUT input = inputSupplier.get();
        OUTPUT output = null;

        // if there are retries left and the worker has never run or if the output isn't a full success keep
        // trying.
        int i = 0;
        while (i < maxRetriesCount && (output == null || !succeeded.test(output))) {
          LOGGER.info("Starting attempt: {}", i);
          if (output != null) {
            input = nextInput.apply(input, output);
          }
          output = worker.run(input, jobRoot);
          i++;
        }
        outputFuture.complete(output);
      } catch (Throwable e) {
        LOGGER.info("Completing future exceptionally...", e);
        outputFuture.completeExceptionally(e);
      }
    });
  }

}
