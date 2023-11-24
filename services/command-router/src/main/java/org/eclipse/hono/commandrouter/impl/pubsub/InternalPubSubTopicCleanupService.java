/**
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * https://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.hono.commandrouter.impl.pubsub;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.hono.client.pubsub.PubSubBasedAdminClientManager;
import org.eclipse.hono.client.pubsub.PubSubConfigProperties;
import org.eclipse.hono.commandrouter.AdapterInstanceStatusService;
import org.eclipse.hono.util.CommandConstants;
import org.eclipse.hono.util.LifecycleStatus;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.core.FixedCredentialsProvider;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

/**
 * A service to delete obsolete {@link CommandConstants#INTERNAL_COMMAND_ENDPOINT} topics and subscriptions.
 */
public class InternalPubSubTopicCleanupService extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(InternalPubSubTopicCleanupService.class);
    private static final long CHECK_INTERVAL_MILLIS = TimeUnit.HOURS.toMillis(6);
    private final Set<String> topicsToDelete = new HashSet<>();
    private final Set<String> subscriptionsToDelete = new HashSet<>();
    private final AdapterInstanceStatusService adapterInstanceStatusService;
    private final LifecycleStatus lifecycleStatus = new LifecycleStatus();
    private final PubSubBasedAdminClientManager adminClientManager;

    private long timerId;

    /**
     * Creates an InternalPubSubTopicCleanupService.
     *
     * @param adapterInstanceStatusService The service providing info about the status of adapter instances.
     * @param pubSubConfigProperties The Pub/Sub config properties containing the Google project ID.
     * @param credentialsProvider The provider for credentials to use for authenticating to the Pub/Sub service.
     * @param vertx The vert.x instance to use.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public InternalPubSubTopicCleanupService(
            final AdapterInstanceStatusService adapterInstanceStatusService,
            final PubSubConfigProperties pubSubConfigProperties,
            final FixedCredentialsProvider credentialsProvider,
            final Vertx vertx) {
        Objects.requireNonNull(pubSubConfigProperties);
        Objects.requireNonNull(credentialsProvider);
        this.adapterInstanceStatusService = Objects.requireNonNull(adapterInstanceStatusService);
        this.adminClientManager = new PubSubBasedAdminClientManager(pubSubConfigProperties, credentialsProvider, vertx);
    }

    /**
     * Creates an InternalPubSubTopicCleanupService. To be used for Unittests.
     *
     * @param adapterInstanceStatusService The service providing info about the status of adapter instances.
     * @param adminClientManager The Pub/Sub based admin client manager which manage topics and subscriptions.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public InternalPubSubTopicCleanupService(
            final AdapterInstanceStatusService adapterInstanceStatusService,
            final PubSubBasedAdminClientManager adminClientManager) {
        this.adapterInstanceStatusService = Objects.requireNonNull(adapterInstanceStatusService);
        this.adminClientManager = Objects.requireNonNull(adminClientManager);
    }

    /**
     * Checks if this service is ready to be used.
     *
     * @return The result of the check.
     */
    public HealthCheckResponse checkReadiness() {
        return HealthCheckResponse.builder()
                .name("Pubsub-topic-cleanup-service")
                .status(lifecycleStatus.isStarted())
                .build();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this component is already started or is currently stopping.
     */
    @Override
    public void start() {
        if (lifecycleStatus.isStarting()) {
            return;
        } else if (!lifecycleStatus.setStarting()) {
            throw new IllegalStateException("client is already started/stopping");
        }
        Optional.of(adminClientManager)
                .ifPresent(i -> {
                    timerId = vertx.setPeriodic(CHECK_INTERVAL_MILLIS, tid -> Vertx.currentContext().executeBlocking(promise -> {
                        LOG.info("running InternalPubSubTopicCleanupService...");
                        performTopicCleanup();
                        performSubscriptionCleanup();
                        promise.complete();
                    }));
                    LOG.info("started InternalPubSubTopicCleanupService");
                    lifecycleStatus.setStarted();
                });
    }

    /**
     * {@inheritDoc}
     * <p>
     * Closes the Pub/Sub admin client.
     *
     * @param stopResult The handler to invoke with the outcome of the operation. The result will be succeeded once this
     *            component is stopped.
     */
    @Override
    public void stop(final Promise<Void> stopResult) {
        lifecycleStatus.runStopAttempt(() -> {
            vertx.cancelTimer(timerId);
            return vertx.executeBlocking(promise -> {
                        adminClientManager.closeAdminClients();
                        promise.complete();
            });
        }).onComplete(stopResult);
    }

    /**
     * Determine topics to be deleted and delete the set of such topics determined in a previous invocation.
     */
    protected final void performTopicCleanup() {
        if (topicsToDelete.isEmpty()) {
            determineToBeDeletedTopics();
        } else {
            adminClientManager.listTopics()
                    .onFailure(t -> LOG.warn("error listing topics", t))
                    .onSuccess(allTopics -> {
                        final List<String> existingTopicsToDelete = topicsToDelete.stream()
                                .filter(allTopics::contains)
                                .collect(Collectors.toList());
                        if (existingTopicsToDelete.isEmpty()) {
                            topicsToDelete.clear();
                            determineToBeDeletedTopics(allTopics);
                        } else {
                            this.adminClientManager.deleteTopics(existingTopicsToDelete)
                                    .onSuccess(v -> LOG.debug("successfully deleted {} topics",
                                            existingTopicsToDelete.size()))
                                    .onFailure(thr -> LOG.warn("Error deleting topics", thr))
                                    .onComplete(ar -> {
                                        topicsToDelete.clear();
                                        determineToBeDeletedTopics();
                                    });
                        }
                    });
        }
    }

    /**
     * Determine subscriptions to be deleted and delete the set of such subscriptions determined in a previous
     * invocation.
     */
    protected final void performSubscriptionCleanup() {
        if (subscriptionsToDelete.isEmpty()) {
            determineToBeDeletedSubscriptions();
        } else {
            adminClientManager.listSubscriptions()
                    .onFailure(t -> LOG.warn("error listing subscriptions", t))
                    .onSuccess(allSubscriptions -> {
                        final List<String> existingSubscriptionsToDelete = subscriptionsToDelete.stream()
                                .filter(allSubscriptions::contains)
                                .collect(Collectors.toList());
                        if (existingSubscriptionsToDelete.isEmpty()) {
                            subscriptionsToDelete.clear();
                            determineToBeDeletedSubscriptions(allSubscriptions);
                        } else {
                            adminClientManager.deleteSubscriptions(existingSubscriptionsToDelete)
                                    .onSuccess(v -> LOG.debug("successfully deleted {} subscriptions",
                                            existingSubscriptionsToDelete.size()))
                                    .onFailure(thr -> LOG.warn("Error deleting subscriptions", thr))
                                    .onComplete(ar -> {
                                        subscriptionsToDelete.clear();
                                        determineToBeDeletedSubscriptions();
                                    });
                        }
                    });
        }
    }

    private void determineToBeDeletedTopics() {
        adminClientManager.listTopics()
                .onSuccess(this::determineToBeDeletedTopics)
                .onFailure(thr -> LOG.warn("error listing topics", thr));
    }

    private void determineToBeDeletedSubscriptions() {
        adminClientManager.listSubscriptions()
                .onSuccess(this::determineToBeDeletedSubscriptions)
                .onFailure(thr -> LOG.warn("error listing subscriptions", thr));
    }

    private void determineToBeDeletedTopics(final Set<String> allTopics) {
        final Map<String, String> adapterInstanceIdToTopicMap = new HashMap<>();
        for (final String topic : allTopics) {
            if (topic.contains(CommandConstants.INTERNAL_COMMAND_ENDPOINT)) {
                try {
                    final String[] ar = topic.split("/");
                    final String adapterInstanceId = ar[3].split("\\.")[0];
                    adapterInstanceIdToTopicMap.put(adapterInstanceId, topic);
                } catch (ArrayIndexOutOfBoundsException e) {
                    LOG.warn("Topic {} does not match usual pattern", topic, e);
                }
            }
        }
        adapterInstanceStatusService.getDeadAdapterInstances(adapterInstanceIdToTopicMap.keySet())
                .onFailure(thr -> LOG.warn("error determining dead adapter instances", thr))
                .onSuccess(deadAdapterInstances -> {
                    deadAdapterInstances.forEach(id -> topicsToDelete.add(adapterInstanceIdToTopicMap.get(id)));
                    if (topicsToDelete.isEmpty()) {
                        LOG.debug("found no topics to be deleted; no. of checked topics: {}",
                                adapterInstanceIdToTopicMap.size());
                    }
                });
    }

    private void determineToBeDeletedSubscriptions(final Set<String> allSubscriptions) {
        final Map<String, String> adapterInstanceIdSubscriptionMap = new HashMap<>();
        for (final String subscription : allSubscriptions) {
            if (subscription.contains(CommandConstants.INTERNAL_COMMAND_ENDPOINT)) {
                try {
                    final String[] ar = subscription.split("/");
                    final String adapterInstanceId = ar[3].split("\\.")[0];
                    adapterInstanceIdSubscriptionMap.put(adapterInstanceId, subscription);
                } catch (ArrayIndexOutOfBoundsException e) {
                    LOG.warn("Subscription {} does not match usual pattern", subscription, e);
                }
            }
        }
        adapterInstanceStatusService.getDeadAdapterInstances(adapterInstanceIdSubscriptionMap.keySet())
                .onFailure(thr -> LOG.warn("error determining dead adapter instances", thr))
                .onSuccess(deadAdapterInstances -> {
                    deadAdapterInstances
                            .forEach(id -> subscriptionsToDelete.add(adapterInstanceIdSubscriptionMap.get(id)));
                    if (subscriptionsToDelete.isEmpty()) {
                        LOG.debug("found no subscriptions to be deleted; no. of checked subscriptions: {}",
                                adapterInstanceIdSubscriptionMap.size());
                    }
                });
    }

}
