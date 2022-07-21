/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.action.ActionType;

/**
 * Action to get weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public final class ClusterPutWRRWeightsAction extends ActionType<ClusterPutWRRWeightsResponse> {

    public static final ClusterPutWRRWeightsAction INSTANCE = new ClusterPutWRRWeightsAction();
    public static final String NAME = "cluster:admin/shard_routing/weights/put";

    private ClusterPutWRRWeightsAction() {
        super(NAME, ClusterPutWRRWeightsResponse::new);
    }


}
