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
    private List<WRRWeight> wrrWeights;
    private boolean weighAwayEnabled;
    private boolean decommissionEnabled;

    ClusterGetWRRWeightsResponse(Object localNodeWeight, boolean weighAwayEnabled, boolean decommissionEnabled) throws IOException {
        this.localNodeWeight = localNodeWeight;
        this.weighAwayEnabled = weighAwayEnabled;
        this.decommissionEnabled = decommissionEnabled;
    }

    ClusterGetWRRWeightsResponse(List<WRRWeight> wrrWeights)
    {
        this.wrrWeights = wrrWeights;

    }


    ClusterGetWRRWeightsResponse(StreamInput in) throws IOException {
         this.localNodeWeight = in.read();
         this.wrrWeights = in.readList(WRRWeight::new);
    }

    /**
     * List of weights to return
     *
     * @return list or weights
     */
    public List<WRRWeight> weights() {
        return this.wrrWeights;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(wrrWeights);
        out.writeInt((Integer) localNodeWeight);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if(this.wrrWeights!=null && !this.wrrWeights.isEmpty() && localNodeWeight==null)
        {
            for(WRRWeight wt : this.wrrWeights)
            {
                builder.startObject(wt.attributeName());
                for (Map.Entry<String, Object> entry : wt.weights().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }
        }
        else
        {
            builder.field("weight", localNodeWeight.toString());
            builder.startObject("reason");

            builder.field("weight_away",weighAwayEnabled ? 1:0);
            builder.field("decommission", decommissionEnabled?1:0);
            builder.endObject();

        }


        builder.endObject();

//        wrrWeights.toXContent(
//            builder, params);
//        builder.endObject();
        return builder;
    }

    public static ClusterGetWRRWeightsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return new ClusterGetWRRWeightsResponse((List<WRRWeight>) WeightedRoundRobinMetadata.fromXContent(parser));
    }
}
