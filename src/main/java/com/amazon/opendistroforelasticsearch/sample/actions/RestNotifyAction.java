package com.amazon.opendistroforelasticsearch.sample.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestNotifyAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(RestNotifyAction.class);

    @Override
    public String getName(){
        return "notifications";
    }

    @Inject
    public RestNotifyAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/notify", this);
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client)  {
        try {
            /*
            log.info("Notifications request headers ....");
            restRequest.getHeaders().forEach((k,v)->{
                log.info(k + " : " + v);
            });
            */
            log.info("Received Notification: "+restRequest.content().utf8ToString());
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "ack"));
        } catch (final Exception ex){
            log.error("Notifications: error", ex);
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST,
                    ex.getMessage() == null ? "Unknown" : ex.getMessage()));
        }
    }
}
