package com.example.temporal;

import com.example.temporal.activity.OrderActivity;
import com.example.temporal.dto.OrderRequest;
import com.example.temporal.dto.OrderResponse;
import com.example.temporal.workflow.OrderWorkflow;
import com.example.temporal.workflow.OrderWorkflowImpl;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class OrderWorkflowTest {

    private TestWorkflowEnvironment testEnv;
    private Worker worker;
    private OrderActivity activities;

    static class TestOrderActivity implements OrderActivity {
        private String lastProcessedOrderId;
        private double lastProcessedAmount;
        private String lastNotifiedEmail;

        @Override
        public boolean processPayment(String orderId, double amount) {
            this.lastProcessedOrderId = orderId;
            this.lastProcessedAmount = amount;
            return true;
        }

        @Override
        public void sendNotification(String orderId, String customerEmail) {
            this.lastNotifiedEmail = customerEmail;
        }
    }

    @BeforeEach
    void setUp() {
        testEnv = TestWorkflowEnvironment.newInstance();
        worker = testEnv.newWorker(OrderWorkflow.TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);

        activities = new TestOrderActivity();
        worker.registerActivitiesImplementations(activities);

        testEnv.start();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    @Test
    void testOrderWorkflowSuccess() {
        OrderWorkflow workflow = testEnv.getWorkflowClient().newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(OrderWorkflow.TASK_QUEUE).build()
        );

        OrderRequest request = new OrderRequest("1001", "test@example.com", 99.99);
        OrderResponse response = workflow.processOrder(request);

        assertEquals("COMPLETED", response.status());
        assertEquals("1001", response.orderId());

        TestOrderActivity testActivities = (TestOrderActivity) activities;
        assertEquals("1001", testActivities.lastProcessedOrderId);
        assertEquals(99.99, testActivities.lastProcessedAmount);
        assertEquals("test@example.com", testActivities.lastNotifiedEmail);
    }
}
