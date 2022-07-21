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
import org.opensearch.action.admin.cluster.shards.routing.wrr.get.ClusterGetWRRWeightsRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestClusterGetWRRWeightsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterPutWRRWeightsAction.class);
    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/shard_routing/weights"));
    }


    @Override
    public String getName() {
        return "get_wrrweights_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterGetWRRWeightsRequest getWRRWeightsRequest = Requests.getWRRWEightsRequest();
        getWRRWeightsRequest.local(request.paramAsBoolean("local", getWRRWeightsRequest.local()));
        return channel -> client.admin()
            .cluster()
            .getWRRWeights(getWRRWeightsRequest, new RestToXContentListener<>(channel));
    }

}

