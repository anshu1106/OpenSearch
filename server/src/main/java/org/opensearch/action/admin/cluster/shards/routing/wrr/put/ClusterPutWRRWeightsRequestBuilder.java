/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;


import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Request builder to update weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterPutWRRWeightsRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterPutWRRWeightsRequest,
    ClusterPutWRRWeightsResponse,
    ClusterPutWRRWeightsRequestBuilder> {
    public ClusterPutWRRWeightsRequestBuilder(OpenSearchClient client, ClusterPutWRRWeightsAction action) {
        super(client, action, new ClusterPutWRRWeightsRequest());
    }

}
