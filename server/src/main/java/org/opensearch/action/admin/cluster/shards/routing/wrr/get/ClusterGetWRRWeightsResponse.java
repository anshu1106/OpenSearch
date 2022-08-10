/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionResponse;

import org.opensearch.cluster.metadata.WeightedRoundRobinMetadata;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response from fetching weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWRRWeightsResponse extends ActionResponse implements ToXContentObject {
    private Object localNodeWeight;
    private WRRWeight wrrWeights;
    private boolean weighAwayEnabled;
    private boolean decommissionEnabled;
    private WeightedRoundRobinMetadata weightedRoundRobinMetadata;

    ClusterGetWRRWeightsResponse(Object localNodeWeight, boolean weighAwayEnabled, boolean decommissionEnabled) throws IOException {
        this.localNodeWeight = localNodeWeight;
        this.weighAwayEnabled = weighAwayEnabled;
        this.decommissionEnabled = decommissionEnabled;
    }

    ClusterGetWRRWeightsResponse(WRRWeight wrrWeights)
    {
        this.wrrWeights = wrrWeights;

    }
    ClusterGetWRRWeightsResponse(WeightedRoundRobinMetadata metadata)
    {
        this.weightedRoundRobinMetadata = metadata;
        this.wrrWeights = metadata.getWrrWeight();
    }


    ClusterGetWRRWeightsResponse(StreamInput in) throws IOException {
//         this.localNodeWeight = in.read();
//         this.weighAwayEnabled = in.readBoolean();
//         this.decommissionEnabled = in.readBoolean();
         this.wrrWeights = new WRRWeight(in);
    }

    /**
     * List of weights to return
     *
     * @return list or weights
     */
    public WRRWeight weights() {
        return this.wrrWeights;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if(wrrWeights!=null)
        {
            wrrWeights.writeTo(out);

        }
//        if(localNodeWeight!=null)
//        {
//            out.writeInt((Integer) localNodeWeight);
//
//        }
//        out.writeBoolean(weighAwayEnabled);
//        out.writeBoolean(decommissionEnabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if(this.wrrWeights!=null  && this.localNodeWeight==null)
        {
                builder.startObject("awareness");
                builder.startObject(wrrWeights.attributeName());
                for (Map.Entry<String, Object> entry : wrrWeights.weights().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
                builder.endObject();
        }
        else
        {
            builder.field("weight", this.localNodeWeight.toString());
//            builder.startObject("reason");
//
//            builder.field("weight_away",this.weighAwayEnabled ? 1:0);
//            builder.field("decommission", this.decommissionEnabled?1:0);
//            builder.endObject();

        }


        builder.endObject();

//        wrrWeights.toXContent(
//            builder, params);
//        builder.endObject();
        return builder;
    }

    public static ClusterGetWRRWeightsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return new ClusterGetWRRWeightsResponse(WeightedRoundRobinMetadata.fromXContent(parser));
    }
}
