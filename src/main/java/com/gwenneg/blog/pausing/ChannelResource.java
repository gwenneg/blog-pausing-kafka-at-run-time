package com.gwenneg.blog.pausing;

import jakarta.inject.Inject;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

// FIXME Make sure that the endpoints in this class are secured and accessible only to authorized users!
@Path("/channels")
public class ChannelResource {

    @Inject
    ChannelFlowController channelFlowController;

    @PUT
    @Path("/pause")
    public Response pause(String channel) {
        try {
            channelFlowController.pause(channel);
            return Response.ok().build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(e.getMessage())
                .build();
        }
    }

    @PUT
    @Path("/resume")
    public Response resume(String channel) {
        try {
            channelFlowController.resume(channel);
            return Response.ok().build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(e.getMessage())
                .build();
        }
    }
}
