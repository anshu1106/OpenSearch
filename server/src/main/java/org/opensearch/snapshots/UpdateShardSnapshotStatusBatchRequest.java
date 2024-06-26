/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Internal request that is used to send changes in snapshot status to master
 *
 * @opensearch.internal
 */
public class UpdateShardSnapshotStatusBatchRequest extends ClusterManagerNodeRequest<UpdateShardSnapshotStatusBatchRequest> {
    private final List<UpdateIndexShardSnapshotStatusRequest> requests;

    public UpdateShardSnapshotStatusBatchRequest(List<UpdateIndexShardSnapshotStatusRequest> requests) {
        this.requests = requests;
    }

    public List<UpdateIndexShardSnapshotStatusRequest> getRequests() {
        return requests;
    }

    public UpdateShardSnapshotStatusBatchRequest(StreamInput in) throws IOException {
        super(in);
        requests = in.readList(UpdateIndexShardSnapshotStatusRequest::new);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(requests);
    }

}
