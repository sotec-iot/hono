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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.hono.client.command.CommandRoutingUtil;
import org.eclipse.hono.client.pubsub.PubSubBasedAdminClientManager;
import org.eclipse.hono.commandrouter.AdapterInstanceStatusService;
import org.eclipse.hono.test.VertxMockSupport;
import org.eclipse.hono.util.CommandConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Verifies behavior of {@link InternalPubSubTopicCleanupService}.
 */
public class InternalPubSubTopicCleanupServiceTest {

    private final String PROJECT_ID = "test-project";
    private InternalPubSubTopicCleanupService internalPubSubTopicCleanupService;
    private AdapterInstanceStatusService adapterInstanceStatusService;
    private PubSubBasedAdminClientManager adminClientManager;

    /**
     * Sets up fixture.
     */
    @BeforeEach
    public void setUp() {
        final Vertx vertx = mock(Vertx.class);
        final Context context = VertxMockSupport.mockContext(vertx);
        adapterInstanceStatusService = mock(AdapterInstanceStatusService.class);
        adminClientManager = mock(PubSubBasedAdminClientManager.class);

        internalPubSubTopicCleanupService = new InternalPubSubTopicCleanupService(adapterInstanceStatusService,
                adminClientManager);
        internalPubSubTopicCleanupService.init(vertx, context);
    }

    /**
     * Verifies that the service deletes topics identified as obsolete.
     */
    @Test
    void testPerformTopicCleanup() {

        final AtomicInteger counter = new AtomicInteger();
        final String podName = "myAdapter";
        final String aliveContainerId = "0ad9864b08bf";
        final String deadContainerId = "000000000000";

        final Set<String> toBeDeletedCmdInternalTopics = new HashSet<>();
        toBeDeletedCmdInternalTopics.add(getCommandInternalTopic(podName, deadContainerId, counter.getAndIncrement()));
        toBeDeletedCmdInternalTopics.add(getCommandInternalTopic(podName, deadContainerId, counter.getAndIncrement()));
        toBeDeletedCmdInternalTopics.add(getCommandInternalTopic(podName, deadContainerId, counter.getAndIncrement()));

        final Set<String> allTopics = new HashSet<>(toBeDeletedCmdInternalTopics);
        allTopics.add("other");
        allTopics.add(getCommandInternalTopic(podName, aliveContainerId, counter.getAndIncrement()));
        allTopics.add(getCommandInternalTopic(podName, aliveContainerId, counter.getAndIncrement()));
        allTopics.add(getCommandInternalTopic(podName, aliveContainerId, counter.getAndIncrement()));

        when(adapterInstanceStatusService.getDeadAdapterInstances(any()))
                .thenAnswer(invocation -> {
                    final Collection<String> adapterInstanceIdsParam = invocation.getArgument(0);
                    final Set<String> deadIds = adapterInstanceIdsParam.stream()
                            .filter(id -> id.contains(deadContainerId)).collect(Collectors.toSet());
                    return Future.succeededFuture(deadIds);
                });

        when(adminClientManager.deleteTopics(any()))
                .thenAnswer(invocation -> {
                    final List<String> topicsToDeleteParam = invocation.getArgument(0);
                    topicsToDeleteParam.forEach(allTopics::remove);
                    return Future.succeededFuture();
                });

        when(adminClientManager.listTopics()).thenReturn(Future.succeededFuture(allTopics));

        internalPubSubTopicCleanupService.performTopicCleanup();
        verify(adminClientManager, never()).deleteTopics(any());

        internalPubSubTopicCleanupService.performTopicCleanup();
        final var deletedTopicsCaptor = ArgumentCaptor.forClass(List.class);

        verify(adminClientManager).deleteTopics(deletedTopicsCaptor.capture());
        assertThat(deletedTopicsCaptor.getValue()).isEqualTo(new ArrayList<>(toBeDeletedCmdInternalTopics));
    }

    /**
     * Verifies that the service deletes topics identified as obsolete.
     */
    @Test
    void testPerformSubscriptiopnsCleanup() {

        final AtomicInteger counter = new AtomicInteger();
        final String podName = "myAdapter";
        final String aliveContainerId = "0ad9864b08bf";
        final String deadContainerId = "000000000000";

        final Set<String> toBeDeletedCmdInternalSubscriptions = new HashSet<>();
        toBeDeletedCmdInternalSubscriptions
                .add(getCommandInternalSubscription(podName, deadContainerId, counter.getAndIncrement()));
        toBeDeletedCmdInternalSubscriptions
                .add(getCommandInternalSubscription(podName, deadContainerId, counter.getAndIncrement()));
        toBeDeletedCmdInternalSubscriptions
                .add(getCommandInternalSubscription(podName, deadContainerId, counter.getAndIncrement()));

        final Set<String> allSubscriptions = new HashSet<>(toBeDeletedCmdInternalSubscriptions);
        allSubscriptions.add("other");
        allSubscriptions.add(getCommandInternalSubscription(podName, aliveContainerId, counter.getAndIncrement()));
        allSubscriptions.add(getCommandInternalSubscription(podName, aliveContainerId, counter.getAndIncrement()));
        allSubscriptions.add(getCommandInternalSubscription(podName, aliveContainerId, counter.getAndIncrement()));

        when(adapterInstanceStatusService.getDeadAdapterInstances(any()))
                .thenAnswer(invocation -> {
                    final Collection<String> adapterInstanceIdsParam = invocation.getArgument(0);
                    final Set<String> deadIds = adapterInstanceIdsParam.stream()
                            .filter(id -> id.contains(deadContainerId)).collect(Collectors.toSet());
                    return Future.succeededFuture(deadIds);
                });

        when(adminClientManager.deleteSubscriptions(any()))
                .thenAnswer(invocation -> {
                    final List<String> topicsToDeleteParam = invocation.getArgument(0);
                    topicsToDeleteParam.forEach(allSubscriptions::remove);
                    return Future.succeededFuture();
                });

        when(adminClientManager.listSubscriptions()).thenReturn(Future.succeededFuture(allSubscriptions));

        internalPubSubTopicCleanupService.performSubscriptionCleanup();
        verify(adminClientManager, never()).deleteSubscriptions(any());

        internalPubSubTopicCleanupService.performSubscriptionCleanup();
        final var deletedSubscriptionsCaptor = ArgumentCaptor.forClass(List.class);

        verify(adminClientManager).deleteSubscriptions(deletedSubscriptionsCaptor.capture());
        assertThat(deletedSubscriptionsCaptor.getValue())
                .isEqualTo(new ArrayList<>(toBeDeletedCmdInternalSubscriptions));
    }

    private String getCommandInternalTopic(final String podName, final String containerId, final int counter) {
        final String adapterInstanceId = CommandRoutingUtil.getNewAdapterInstanceIdForK8sEnv(podName, containerId,
                counter);

        return String.format("projects/%s/topics/%s.%s", PROJECT_ID, adapterInstanceId,
                CommandConstants.INTERNAL_COMMAND_ENDPOINT);
    }

    private String getCommandInternalSubscription(final String podName, final String containerId, final int counter) {
        final String adapterInstanceId = CommandRoutingUtil.getNewAdapterInstanceIdForK8sEnv(podName, containerId,
                counter);

        return String.format("projects/%s/subscriptions/%s.%s", PROJECT_ID, adapterInstanceId,
                CommandConstants.INTERNAL_COMMAND_ENDPOINT);
    }
}
