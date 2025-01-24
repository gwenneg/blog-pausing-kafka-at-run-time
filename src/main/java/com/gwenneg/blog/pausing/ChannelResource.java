package com.gwenneg.blog.pausing;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

@Path("/channels")
public class ChannelResource {

    @Inject
    ChannelRegistry channelRegistry;

    @PUT
    @Path("/pause")
    public void pause(String channel) {
        PausableChannel pausableChannel = getPausableChannel(channel);
        if (!pausableChannel.isPaused()) {
            pausableChannel.pause();
            Log.infof("Paused channel: %s", channel);
        }
    }

    @PUT
    @Path("/resume")
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
            throw new NotFoundException("Channel not found or not marked as pausable in application.properties");
        } else {
            return pausableChannel;
        }
    }
}
