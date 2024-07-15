package org.nashtech.com.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollection;

public class PubSubReader {
    private final String subscription;

    public PubSubReader(String subscription) {
        this.subscription = subscription;
    }

    public PCollection<String> readMessages(Pipeline pipeline) {
        return pipeline.apply("Read from Pub/Sub",
                PubsubIO.readStrings().fromSubscription(subscription));
    }
}