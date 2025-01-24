package com.gwenneg.blog.pausing;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ChannelFlowController {

    @Inject
    ChannelRegistry channelRegistry;

    public void pause(String channel) {
        PausableChannel pausableChannel = getPausableChannel(channel);
        if (!pausableChannel.isPaused()) {
            pausableChannel.pause();
            Log.infof("Paused channel: %s", channel);
        }
    }

    public void resume(String channel) {
        PausableChannel pausableChannel = getPausableChannel(channel);
        if (pausableChannel.isPaused()) {
            pausableChannel.resume();
            Log.infof("Resumed channel: %s", channel);
        }
    }

    private PausableChannel getPausableChannel(String channel) {
        PausableChannel pausableChannel = channelRegistry.getPausable(channel);
        if (pausableChannel == null) {
            throw new IllegalArgumentException("Channel not found or not marked as pausable from the Quarkus configuration");
        } else {
            return pausableChannel;
        }
    }
}
