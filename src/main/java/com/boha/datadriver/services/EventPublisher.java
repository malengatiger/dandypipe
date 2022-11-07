package com.boha.datadriver.services;

import com.boha.datadriver.util.E;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Publishes events to PubSub topics
 */
@Service
public class EventPublisher {
    static final Logger LOGGER = Logger.getLogger(EventPublisher.class.getSimpleName());



    private String projectId;
    @Value("${eventTopicId}")
    private String eventTopicId;
    @Value("${flatTopicId}")
    private String flatTopicId;
    @Value("${bigQueryTopicId}")
    private String bigQueryTopicId;
    @Value("${pullTopicId}")
    private String pullTopicId;
    Publisher publisher;
    Publisher flatPublisher;

    Publisher bigQueryPublisher;
    Publisher pullPublisher;

    @Autowired
    private Environment environment;
    void setProjectId() {
        projectId = environment.getProperty("PROJECT_ID");
    }


    public void publish(String message)
            throws Exception {
        setProjectId();
        String name = "projects/"+ projectId + "/topics/"+eventTopicId;
        TopicName topicName = TopicName.of(projectId, name);

        if (publisher == null) {
            publisher = Publisher.newBuilder(topicName.getTopic()).build();
            LOGGER.info(E.BLUE_DOT + E.BLUE_DOT +
                    " Published toTopic: " + topicName.getTopic()
                    + " " + E.GREEN_APPLE);
        }
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        // Once published, returns a server-assigned message id (unique within the topic)
        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        LOGGER.info("PubSub message published " + E.AMP + E.AMP + E.AMP + " id: " + messageIdFuture.get() + " topic: " + topicName.getTopic());

    }

    public void publishFlatEvent(String message)
            throws Exception {
        setProjectId();
        String name = "projects/"+ projectId + "/topics/"+flatTopicId;

        TopicName topicName = TopicName.of(projectId, name);

        if (flatPublisher == null) {
            flatPublisher = Publisher.newBuilder(topicName.getTopic()).build();
            LOGGER.info(E.RED_DOT + E.RED_DOT +E.RED_DOT +
                    " FlatPublisher created with Topic: " + topicName.getTopic()
                    + " " + E.GREEN_APPLE);

        }
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        // Once published, returns a server-assigned message id (unique within the topic)
        ApiFuture<String> messageIdFuture = flatPublisher.publish(pubsubMessage);
        LOGGER.info("PubSub flat message published " + E.RED_APPLE + E.RED_APPLE + E.RED_APPLE
                + " messageId: " + messageIdFuture.get() + " topic: " + topicName.getTopic());

    }
    public void publishPull(String message)
            throws Exception {
        setProjectId();
        String name = "projects/"+ projectId + "/topics/"+pullTopicId;

        TopicName topicName = TopicName.of(projectId, name);

        if (pullPublisher == null) {
            pullPublisher = Publisher.newBuilder(topicName.getTopic()).build();
            LOGGER.info(E.RED_DOT + E.RED_DOT +E.RED_DOT +
                    " PullPublisher created with Topic: " + topicName.getTopic()
                    + " " + E.GREEN_APPLE);

        }
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        // Once published, returns a server-assigned message id (unique within the topic)
        ApiFuture<String> messageIdFuture = pullPublisher.publish(pubsubMessage);
        LOGGER.info("PubSub Pull message published " + E.RED_APPLE + E.RED_APPLE + E.RED_APPLE
                + " messageId: " + messageIdFuture.get() + " topic: " + topicName.getTopic());

    }
    public void publishBigQueryEvent(String message)
            throws Exception {
        setProjectId();
        String name = "projects/"+ projectId + "/topics/"+bigQueryTopicId;
        TopicName topicName = TopicName.of(projectId, name);

        if (bigQueryPublisher == null) {
            bigQueryPublisher = Publisher.newBuilder(topicName.getTopic()).build();
            LOGGER.info(E.RED_DOT + E.RED_DOT +E.RED_DOT +
                    " BigQueryPublisher created with Topic: " + topicName.getTopic()
                    + " " + E.GREEN_APPLE);

        }
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        // Once published, returns a server-assigned message id (unique within the topic)
        ApiFuture<String> messageIdFuture = bigQueryPublisher.publish(pubsubMessage);
        LOGGER.info("PubSub BigQuery message published " + E.ORANGE_HEART + E.ORANGE_HEART + E.ORANGE_HEART
                + " messageId: " + messageIdFuture.get() + " topic: " + topicName.getTopic());

    }

    void closePublisher() throws InterruptedException {
        if (publisher != null) {
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
        }
    }
}
