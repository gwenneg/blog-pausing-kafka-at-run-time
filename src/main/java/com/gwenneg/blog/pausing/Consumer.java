package com.gwenneg.blog.pausing;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.Set;

@ApplicationScoped
public class Consumer {

    public static final String PAUSING_KAFKA_AT_RUNTIME_BLOCKING = "pausing-kafka-at-runtime-blocking";
    public static final String PAUSING_KAFKA_AT_RUNTIME_NON_BLOCKING = "pausing-kafka-at-runtime-non-blocking";

    private final Set<String> received = new ConcurrentHashSet<>();

    @Incoming(PAUSING_KAFKA_AT_RUNTIME_BLOCKING)
    public void consumeBlocking(String message) {
        process(message);
    }

    @Incoming(PAUSING_KAFKA_AT_RUNTIME_NON_BLOCKING)
    public Uni<Void> consumeNonBlocking(Message<String> message) {
        return Uni.createFrom().completionStage(message.ack())
            .invoke(() -> process(message.getPayload()));
    }

    private void process(String message) {
        Log.infof("Received message: %s", message);
        received.add(message);
    }

    public Set<String> getReceived() {
        return received;
    }
}
