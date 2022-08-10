/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.routing.WRRShardRoutingService;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.cluster.routing.WeightedRoundRobin;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WeightedRoundRobinMetadata  extends AbstractNamedDiffable<Metadata.Custom> implements  Metadata.Custom {
    private static final Logger logger = LogManager.getLogger(WeightedRoundRobinMetadata.class);
    public static final String TYPE = "wrr_shard_routing";
    private   WRRWeight wrrWeight;

    public WRRWeight getWrrWeight() {
        return wrrWeight;
    }

    public WeightedRoundRobinMetadata setWrrWeight(WRRWeight wrrWeight) {
        this.wrrWeight = wrrWeight;
        return this;
    }

    public WeightedRoundRobinMetadata(StreamInput in) throws IOException {
        this.wrrWeight = new WRRWeight(in);
    }

    public WeightedRoundRobinMetadata(WRRWeight wrrWeight) {
        this.wrrWeight = wrrWeight;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        //TODO: Check if this needs to be changed
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        //TODO:
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        wrrWeight.writeTo(out);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        logger.info("WeightedRoundRobinMetada::readDiffFrom");

        return readDiffFrom(Metadata.Custom.class, TYPE, in);
        //return new WeightedRoundRobinMetadata.WeightedRoundRobinMetadataDiff(in);
    }

    public static WeightedRoundRobinMetadata fromXContent(XContentParser parser) throws IOException {
        logger.info("WRR metadata from xcontent invoked");
        String attrKey = null;
        Object attrValue;
        String attributeName = null;
        Map<String, Object> weights = new HashMap<>();
        WRRWeight wrrWeight = null;
        XContentParser.Token token;
        // move to the first alias
        parser.nextToken();
        String awarenessField = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                awarenessField = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new OpenSearchParseException("failed to parse wrr metadata  [{}], expected object", awarenessField);
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    attributeName = parser.currentName();
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new OpenSearchParseException("failed to parse wrr metadata  [{}], expected object", attributeName);
                    }

                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            attrKey = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            attrValue = parser.text();
                            weights.put(attrKey, attrValue);
                        } else {
                            throw new OpenSearchParseException("failed to parse wrr metadata attribute [{}], unknown type", attributeName);
                        }
                    }
            }} else {
                throw new OpenSearchParseException("failed to parse wrr metadata attribute [{}]", attributeName);
            }
        }
        logger.info("WRR metadata fromxcontent completing");

        wrrWeight = new WRRWeight(attributeName, weights);
        logger.info("WRR metadata fromxcontent completed");

        return new WeightedRoundRobinMetadata(wrrWeight);

    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeightedRoundRobinMetadata that = (WeightedRoundRobinMetadata) o;

        //if (!name.equals(that.name)) return false;
        boolean found = wrrWeight.equals(that.wrrWeight);
        logger.info("WEightedRoundRobin::EQUALS");
        logger.info(found);
        return found;
        //return false;
    }

    @Override
    public int hashCode() {
        return wrrWeight.hashCode();
    }



    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        toXContent(wrrWeight, builder, params);
        return builder;
    }

    public static void toXContent(WRRWeight wrrWeight, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("awareness");
        builder.startObject(wrrWeight.attributeName());
        for (Map.Entry<String, Object> entry : wrrWeight.weights().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        builder.endObject();
    }
    @Override
    public String toString() {
        return Strings.toString(this);
    }



}
