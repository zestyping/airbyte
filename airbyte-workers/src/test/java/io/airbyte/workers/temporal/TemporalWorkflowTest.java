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

import static java.lang.Thread.sleep;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.filter.v1.WorkflowExecutionFilter;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.QueryWorkflowRequest;
import io.temporal.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// based of example in temporal samples:
// https://github.com/temporalio/samples-java/blob/master/src/test/java/io/temporal/samples/hello/HelloActivityTest.java
@Disabled
class TemporalWorkflowTest {

  private static final String TASK_QUEUE = "a";

  private TestWorkflowEnvironment testEnv;
  private Worker worker;
  private WorkflowClient client;

  @BeforeEach
  public void setUp() {
    testEnv = TestWorkflowEnvironment.newInstance();
    worker = testEnv.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(TestWorkflow.WorkflowImpl.class, TestWorkflow2.WorkflowImpl.class);

    client = testEnv.getWorkflowClient();
  }

  @SuppressWarnings("unchecked")
  @Test
  void test() {
    final Consumer<String> activity1Consumer = mock(Consumer.class);
    final Consumer<String> activity2Consumer = mock(Consumer.class);

    doThrow(new IllegalArgumentException("don't argue!")).when(activity1Consumer).accept(any());

    worker.registerActivitiesImplementations(new TestWorkflow.Activity1Impl(activity1Consumer, null),
        new TestWorkflow.Activity2Impl(activity2Consumer));
    testEnv.start();

    final TestWorkflow workflow = client.newWorkflowStub(TestWorkflow.class, WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build());
    workflow.run();
  }

  // verifies that once an activity fails, no more activities get run.
  @SuppressWarnings("unchecked")
  @Test
  void test2() throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);

    final Thread cancellationThread = new Thread(() -> {
      while (countDownLatch.getCount() > 0) {
        System.out.println("sleeping waiting for cancellation");
        try {
          sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      final WorkflowServiceStubs temporalService = testEnv.getWorkflowService();
      // there should only be one execution running.
      final WorkflowExecutionInfo workflowExecutionInfo = temporalService.blockingStub().listOpenWorkflowExecutions(null).getExecutionsList().get(0);
      final String workflowId = workflowExecutionInfo.getExecution().getWorkflowId();

      final WorkflowExecution workflowExecution = WorkflowExecution.newBuilder()
          .setWorkflowId(workflowId)
          .build();

      final RequestCancelWorkflowExecutionRequest cancelRequest = RequestCancelWorkflowExecutionRequest.newBuilder()
          .setWorkflowExecution(workflowExecution)
          .setNamespace("default")
          .build();

      temporalService.blockingStub().requestCancelWorkflowExecution(cancelRequest);
    });

    final Consumer<String> activity1Consumer = mock(Consumer.class);
    final Consumer<String> activity2Consumer = mock(Consumer.class);

    // doThrow(new IllegalArgumentException("don't argue!")).when(activity1Consumer).accept(any());

    worker.registerActivitiesImplementations(new TestWorkflow.Activity1Impl(activity1Consumer, countDownLatch),
        new TestWorkflow.Activity2Impl(activity2Consumer));
    testEnv.start();

    cancellationThread.start();

    final TestWorkflow workflow = client.newWorkflowStub(TestWorkflow.class, WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build());
    try {
      final String run = workflow.run();
      System.out.println("run = " + run);
    } catch (Exception e) {
      System.out.println("e = " + e);
    }

    cancellationThread.join();
  }

  @SuppressWarnings("unchecked")
  @Test
  void test3() throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);

    final Consumer<String> activity1Consumer = mock(Consumer.class);
    final Consumer<String> activity2Consumer = mock(Consumer.class);

    doAnswer((a) -> {
      countDownLatch.await(1, TimeUnit.MINUTES);
      return null;
    }).when(activity1Consumer).accept(any());

    worker.registerActivitiesImplementations(new TestWorkflow2.Activity1Impl(activity1Consumer, countDownLatch),
        new TestWorkflow2.Activity2Impl(activity2Consumer));
    testEnv.start();

    final TestWorkflow2 workflowStub = client.newWorkflowStub(TestWorkflow2.class, WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build());
    final ImmutablePair<WorkflowExecution, CompletableFuture<String>> pair =
        TemporalUtils.asyncExecute(workflowStub, workflowStub::run, "whatever", String.class);
    final WorkflowExecution workflowExecution = pair.getLeft();
    // final WorkflowExecution workflowExecution = WorkflowClient.start(workflowStub::run, "whatever");
    final String workflowId = workflowExecution.getWorkflowId();
    final String runId = workflowExecution.getRunId();

    System.out.println("workflowId = " + workflowId);
    System.out.println("runId = " + runId);

    final WorkflowServiceStubs temporalService = testEnv.getWorkflowService();
    final QueryWorkflowRequest request = QueryWorkflowRequest
        .newBuilder()
        .setNamespace("default")
        .setExecution(WorkflowExecution.newBuilder()
            .setWorkflowId(workflowId)
            .setRunId(runId)
            .build())
        .build();

    final List<WorkflowExecutionInfo> workflowExecutionInfo = temporalService.blockingStub().listOpenWorkflowExecutions(null).getExecutionsList();
    System.out.println("workflowExecutionInfo = " + workflowExecutionInfo);
    ListOpenWorkflowExecutionsRequest listRequest = ListOpenWorkflowExecutionsRequest.newBuilder()
        .setExecutionFilter(WorkflowExecutionFilter.newBuilder()
            .setWorkflowId(workflowId)
            .build())
        .build();
    final List<WorkflowExecutionInfo> workflowExecutionInfo2 =
        temporalService.blockingStub().listOpenWorkflowExecutions(listRequest).getExecutionsList();
    System.out.println("workflowExecutionInfo2 = " + workflowExecutionInfo2);

    // final QueryWorkflowResponse queryWorkflowResponse =
    // temporalService.blockingStub().queryWorkflow(request);
    // System.out.println("queryWorkflowResponse = " + queryWorkflowResponse);

    countDownLatch.countDown();

    WorkflowStub untyped = WorkflowStub.fromTyped(workflowStub);
    try {
      final CompletableFuture<String> resultAsync = untyped.getResultAsync(String.class);
      // final String s = resultAsync.get(1, TimeUnit.MINUTES);
      final String s = pair.getRight().get(1, TimeUnit.MINUTES);
      System.out.println("s = " + s);
      // String result = untyped.getResult(1, TimeUnit.MINUTES, String.class);
      // System.out.println("result = " + result);
    } catch (TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  // private void cancelWorkflow() {
  // final WorkflowServiceBlockingStub temporalService = testEnv.getWorkflowService().blockingStub();
  // // there should only be one execution running.
  // final String workflowId =
  // temporalService.listOpenWorkflowExecutions(null).getExecutionsList().get(0).getExecution().getWorkflowId();
  //
  // final WorkflowExecution workflowExecution = WorkflowExecution.newBuilder()
  // .setWorkflowId(workflowId)
  // .build();
  //
  // final RequestCancelWorkflowExecutionRequest cancelRequest =
  // RequestCancelWorkflowExecutionRequest.newBuilder()
  // .setWorkflowExecution(workflowExecution)
  // .build();
  //
  // testEnv.getWorkflowService().blockingStub().requestCancelWorkflowExecution(cancelRequest);
  // }

  @AfterEach
  public void tearDown() {
    testEnv.close();
  }

}
