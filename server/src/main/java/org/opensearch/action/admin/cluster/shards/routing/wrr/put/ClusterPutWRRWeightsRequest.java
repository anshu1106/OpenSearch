/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request to update weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterPutWRRWeightsRequest extends AcknowledgedRequest<ClusterPutWRRWeightsRequest> {


    private WRRWeight wrrWeight;

    public WRRWeight wrrWeight() {
        return wrrWeight;
    }

    public ClusterPutWRRWeightsRequest wrrWeight(WRRWeight wrrWeight) {
        this.wrrWeight = wrrWeight;
        return this;
    }

    public ClusterPutWRRWeightsRequest(StreamInput in) throws IOException {
        super(in);
        //attributeName = in.readString();
        wrrWeight = new WRRWeight(in);
    }

    public ClusterPutWRRWeightsRequest() {

    }

    public ClusterPutWRRWeightsRequest setWRRWeight(Map<String, Object> source) {
        try{
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return setWRRWeight(BytesReference.bytes(builder), builder.contentType());
        } catch(IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public ClusterPutWRRWeightsRequest setWRRWeight(BytesReference source, XContentType contentType) {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                contentType
            )
            ) {
            String attrKey = null;
            Object attrValue;
            String attributeName =null;
            Map<String, Object> weights = new HashMap<>();
//            XContentParser.Token token ;
//            parser.nextToken();


            XContentParser.Token token;
            // move to the first alias
            parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    attributeName = parser.currentName();
                }
                else if (token == XContentParser.Token.START_OBJECT){
                    while((token=parser.nextToken())!=XContentParser.Token.END_OBJECT)
                    {
                        if( token == XContentParser.Token.FIELD_NAME)
                        {
                            attrKey = parser.currentName();
                        }
                        else if (token == XContentParser.Token.VALUE_STRING)
                        {
                            attrValue = parser.text();
                            weights.put(attrKey, attrValue);
                        }
                    }


                }
            }
            this.wrrWeight = new WRRWeight(attributeName, weights);
            return this;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

//    //public String attributeName() {
//        return attributeName;
//    }

//    public ClusterPutWRRWeightsRequest attributeName(String attributeName) {
//        this.attributeName = attributeName;
//        return this;
//    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     *
     * @param source weights definition from request body
     * @return this request
     */
    public ClusterPutWRRWeightsRequest source(Map<String, Object> source) {
//        Map<String, Object> weights = null;
//        String attributeName = null;
//        for (Map.Entry<String, Object> entry : wrrWeightsDefinition.entrySet()) {
//            attributeName = entry.getKey();
//            if (!(entry.getValue() instanceof Map)) {
//                throw new IllegalArgumentException("Malformed weights definition, should include an inner object");
//            }
//            weights = (Map<String, Object>) entry.getValue();
//        }
//        this.wrrWeight = new WRRWeight(attributeName, weights);

        for(Map.Entry<String, ?> entry : source.entrySet()) {

            if(!(entry.getValue() instanceof Map)) {
                throw new OpenSearchParseException("key [ awareness] must be an object" );
            }
            setWRRWeight((Map<String,Object>) entry.getValue());
        }
        return this;



    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        wrrWeight.writeTo(out);
    }

    @Override
    public String toString() {
        return "ClusterPutWRRWeightsRequest{"+
            "wrrWeight= "+ wrrWeight.toString()+"}";
    }

}
