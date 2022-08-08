/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionListener;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;

import org.opensearch.cluster.metadata.WeightedRoundRobinMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;

import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportGetWRRWeightsAction extends TransportClusterManagerNodeReadAction<ClusterGetWRRWeightsRequest, ClusterGetWRRWeightsResponse> {

    @Inject
    public TransportGetWRRWeightsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetWRRWeightsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetWRRWeightsRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        //Check threadpool to use
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterGetWRRWeightsResponse read(StreamInput in) throws IOException {
        return new ClusterGetWRRWeightsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetWRRWeightsRequest request, ClusterState state) {

        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }


    @Override
    protected void masterOperation(
        final ClusterGetWRRWeightsRequest request,
        ClusterState state,
        final ActionListener<ClusterGetWRRWeightsResponse> listener
    ) throws IOException {
        Metadata metadata = state.metadata();
        WeightedRoundRobinMetadata weightedRoundRobinMetadata = metadata.custom(WeightedRoundRobinMetadata.TYPE);
        if (request.local()) {
            DiscoveryNode localNode = state.getNodes().getLocalNode();

            boolean weighAwayEnabled = false;
            boolean decommissionEnabled = false;
            Object weight = null;
            //if (localNode.isDataNode() && localNode.getAttributes().containsKey("zone"))
            if (localNode.getAttributes().containsKey("zone")) {

                String zone = localNode.getAttributes().get("zone");
                //Get weight for the zone from weighted round robin metadata

                if (weightedRoundRobinMetadata.getWrrWeight() == null) {
                    weight = 1;
                }
                if (weightedRoundRobinMetadata.getWrrWeight().attributeName() == "zone") {
                    weight = weightedRoundRobinMetadata.getWrrWeight().weights().get(zone);
                    weighAwayEnabled = true;

                }
            }
            listener.onResponse(new ClusterGetWRRWeightsResponse(weight, weighAwayEnabled, decommissionEnabled));
        } else if (weightedRoundRobinMetadata != null) {
            listener.onResponse(new ClusterGetWRRWeightsResponse(weightedRoundRobinMetadata.getWrrWeight()));
        }

    }


}

