package com.gwenneg.blog.pausing;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySource;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.gwenneg.blog.pausing.Consumer.PAUSING_KAFKA_AT_RUN_TIME_BLOCKING;
import static com.gwenneg.blog.pausing.Consumer.PAUSING_KAFKA_AT_RUN_TIME_NON_BLOCKING;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(ChannelResourceTest.class)
public class ChannelResourceTest implements QuarkusTestResourceLifecycleManager {

    @Inject
    @Any
    InMemoryConnector inMemoryConnector;

    @Inject
    Consumer consumer;

    @Override
    public Map<String, String> start() {
        Map<String, String> properties = new HashMap<>();
        properties.putAll(InMemoryConnector.switchIncomingChannelsToInMemory(PAUSING_KAFKA_AT_RUN_TIME_BLOCKING));
        properties.putAll(InMemoryConnector.switchIncomingChannelsToInMemory(PAUSING_KAFKA_AT_RUN_TIME_NON_BLOCKING));
        return properties;
    }

    @Override
    public void stop() {
        InMemoryConnector.clear();
    }

    @AfterEach
    void afterEach() {
        consumer.getReceived().clear();
    }

    @Test
    void testChannelNotFound() {
        pause("unknown", 404);
        resume("unknown", 404);
    }

    @Test
    void testBlocking() {

        InMemorySource<String> inMemorySource = inMemoryConnector.source(PAUSING_KAFKA_AT_RUN_TIME_BLOCKING);

        assertTrue(consumer.getReceived().isEmpty());

        inMemorySource.send("mango");
        inMemorySource.send("apple");

        await()
            .atMost(Duration.ofSeconds(10L))
            .until(() -> consumer.getReceived().size() == 2
                && consumer.getReceived().containsAll(Set.of("mango", "apple")));

        pause(PAUSING_KAFKA_AT_RUN_TIME_BLOCKING, 200);

        sleep(10);

        inMemorySource.send("lemon");
        inMemorySource.send("apricot");
        inMemorySource.send("orange");

        sleep(10);

        /*
         * Although the channel was paused, one more message will be processed because the current version of the
         * PausableChannel API doesn't handle messages that were already requested before the channel is paused.
         * Then, the channel should be fully paused. The SmallRye Reactive Messaging team is actively working on
         * an enhancement to address this issue in the near future.
         */
        assertEquals(3, consumer.getReceived().size());
        assertTrue(consumer.getReceived().contains("lemon"));

        resume(PAUSING_KAFKA_AT_RUN_TIME_BLOCKING, 200);

        await()
            .atMost(Duration.ofSeconds(10L))
            .until(() -> consumer.getReceived().size() == 5
                && consumer.getReceived().containsAll(Set.of("mango", "apple", "lemon", "apricot", "orange")));

        inMemorySource.send("blueberry");
        inMemorySource.send("pear");

        await()
            .atMost(Duration.ofSeconds(10L))
            .until(() -> consumer.getReceived().size() == 7
                && consumer.getReceived().containsAll(Set.of("mango", "apple", "lemon", "apricot", "orange", "blueberry", "pear")));
    }

    @Test
    void testNonBlocking() {

        InMemorySource<String> inMemorySource = inMemoryConnector.source(PAUSING_KAFKA_AT_RUN_TIME_NON_BLOCKING);

        assertTrue(consumer.getReceived().isEmpty());

        inMemorySource.send("carrot");
        inMemorySource.send("corn");

        await()
            .atMost(Duration.ofSeconds(10L))
            .until(() -> consumer.getReceived().size() == 2
                && consumer.getReceived().containsAll(Set.of("carrot", "corn")));

        pause(PAUSING_KAFKA_AT_RUN_TIME_NON_BLOCKING, 200);

        sleep(10);

        inMemorySource.send("mushroom");
        inMemorySource.send("pumpkin");
        inMemorySource.send("broccoli");

        sleep(10);

        /*
         * Although the channel was paused, one more message will be processed because the current version of the
         * PausableChannel API doesn't handle messages that were already requested before the channel is paused.
         * Then, the channel should be fully paused. The SmallRye Reactive Messaging team is actively working on
         * an enhancement to address this issue in the near future.
         */
        assertEquals(3, consumer.getReceived().size());
        assertTrue(consumer.getReceived().contains("mushroom"));

        resume(PAUSING_KAFKA_AT_RUN_TIME_NON_BLOCKING, 200);

        await()
            .atMost(Duration.ofSeconds(10L))
            .until(() -> consumer.getReceived().size() == 5
                && consumer.getReceived().containsAll(Set.of("carrot", "corn", "mushroom", "pumpkin", "broccoli")));

        inMemorySource.send("spinach");
        inMemorySource.send("cucumber");

        await()
            .atMost(Duration.ofSeconds(10L))
            .until(() -> consumer.getReceived().size() == 7
                && consumer.getReceived().containsAll(Set.of("carrot", "corn", "mushroom", "pumpkin", "broccoli", "spinach", "cucumber")));
    }

    private static void pause(String channel, int expectedStatusCode) {
        RestAssured
            .given().body(channel)
            .when().put("/channels/pause")
            .then().statusCode(expectedStatusCode);
    }

    private static void resume(String channel, int expectedStatusCode) {
        RestAssured
            .given().body(channel)
            .when().put("/channels/resume")
            .then().statusCode(expectedStatusCode);
    }

    private static void sleep(long seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
