/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Fetch Weighted Round Robin based shard routing weights
 *
 * @opensearch.api
 *
 */
public class RestClusterGetWeightedRoutingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterGetWeightedRoutingAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/routing/awareness/{attribute}/weights"));
    }

    @Override
    public String getName() {
        return "get_weighted_routing_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterGetWeightedRoutingRequest getWeightedRoutingRequest = Requests.getWeightedRoutingRequest(request.param("attribute"));
        getWeightedRoutingRequest.local(request.paramAsBoolean("local", getWeightedRoutingRequest.local()));
        return channel -> client.admin().cluster().getWeightedRouting(getWeightedRoutingRequest,
            new RestBuilderListener<ClusterGetWeightedRoutingResponse>(channel)  {

        @Override
        public RestResponse buildResponse(ClusterGetWeightedRoutingResponse response, XContentBuilder builder) throws Exception {
            builder.startObject();
            RestActions.buildWeightedRoutingEtagHeader(builder, response.getVersion());
            if (response.getWeightedRouting() != null) {
                for (Map.Entry<String, Double> entry : response.getWeightedRouting().weights().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue().toString());
                }
                if (response.getLocalNodeWeight()!= null) {
                    builder.field(ClusterGetWeightedRoutingResponse.NODE_WEIGHT, response.getLocalNodeWeight());
                }
                //builder.field(WeightedRoutingMetadata.VERSION, response.getVersion());
            }
            builder.endObject();

            RestResponse restResponse = new BytesRestResponse(RestStatus.OK, builder);
            restResponse.addHeader("_version", String.valueOf(response.getVersion()));
            return restResponse;

            //return new BytesRestResponse(RestStatus.OK, builder);
        }
        });
    }
}
