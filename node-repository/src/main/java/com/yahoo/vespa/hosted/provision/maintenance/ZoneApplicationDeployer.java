// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.maintenance;

import com.yahoo.component.Version;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.ApplicationName;
import com.yahoo.config.provision.InstanceName;
import com.yahoo.config.provision.NodeType;
import com.yahoo.config.provision.Zone;
import com.yahoo.vespa.config.server.ApplicationRepository;
import com.yahoo.vespa.config.server.TimeoutBudget;
import com.yahoo.vespa.config.server.http.CompressedApplicationInputStream;
import com.yahoo.vespa.config.server.session.PrepareParams;
import com.yahoo.vespa.config.server.tenant.Tenants;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author freva
 */
public class ZoneApplicationDeployer extends Maintainer {

    private static final Logger log = Logger.getLogger(ZoneApplicationDeployer.class.getName());
    private static final ApplicationId APPLICATION_ID = ApplicationId.from(
            Tenants.HOSTED_VESPA_TENANT, ApplicationName.from("routing"), InstanceName.defaultName());

    private final ApplicationRepository applicationRepository;
    private final Zone zone;
    private final Clock clock;
    private final String artifactoryUrlPrefix;

    public ZoneApplicationDeployer(ApplicationRepository applicationRepository, NodeRepository nodeRepository,
                                   Zone zone, Clock clock, Duration interval, JobControl jobControl,
                                   String artifactoryUrlPrefix) {
        super(nodeRepository, interval, jobControl);

        this.applicationRepository = applicationRepository;
        this.zone = zone;
        this.clock = clock;
        this.artifactoryUrlPrefix = artifactoryUrlPrefix;
    }

    @Override
    protected void maintain() {
        Optional<Version> targetZoneAppVersion = getWantedZoneAppVersion(nodeRepository());
        if (!targetZoneAppVersion.isPresent()) return;
        Version version = targetZoneAppVersion.get();

        log.info("Deploying zone application " + version.toFullString());
        CompressedApplicationInputStream applicationInputStream = getZoneApplicationInputStream(zone, targetZoneAppVersion.get());
        deployZoneApp(applicationInputStream, targetZoneAppVersion.get());
        log.info("Successfully deloyed zone app " + version.toFullString());
    }

    private void deployZoneApp(CompressedApplicationInputStream applicationInputStream, Version version) {
        PrepareParams prepareParams = new PrepareParams.Builder()
                .applicationId(APPLICATION_ID)
                .timeoutBudget(new TimeoutBudget(clock, Duration.ofSeconds(60)))
                .vespaVersion(version.toFullString())
                .build();

        applicationRepository.deploy(applicationInputStream, prepareParams);
    }

    private Optional<Version> getWantedZoneAppVersion(NodeRepository nodeRepository) {
        List<Node> nodes = nodeRepository.getNodes();

        Set<Version> currentConfigServerVersions = filterVespaVersionForNodeType(
                nodes, NodeType.config, ZoneApplicationDeployer::nodeToCurrentVespaVersion);
        Set<Version> wantedProxyVersions = filterVespaVersionForNodeType(
                nodes, NodeType.proxy, ZoneApplicationDeployer::nodeToWantedVespaVersion);

        // Config servers are in process of being upgraded, do not deploy zone app yet
        if (currentConfigServerVersions.size() != 1) return Optional.empty();

        // Config servers' current version is the same as proxy's wanted, nothing new to deploy
        if (currentConfigServerVersions.equals(wantedProxyVersions)) return Optional.empty();

        // Otherwise deploy zone app with config server's current version
        Version currentConfigServerVersion = currentConfigServerVersions.iterator().next();
        return Optional.of(currentConfigServerVersion);
    }

    private CompressedApplicationInputStream getZoneApplicationInputStream(Zone zone, Version version) {
        String filename = String.format(artifactoryUrlPrefix + "zone-application-%s-%s_%s_%s.zip",
                version.toFullString(), zone.system().name(), zone.region().value(), zone.environment().value());
        try {
            return CompressedApplicationInputStream.createFromCompressedStream(
                    new URL(artifactoryUrlPrefix + version.toFullString() + "/" + filename).openStream(),
                    "application/zip");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Set<Version> filterVespaVersionForNodeType(
            List<Node> nodes, NodeType nodeType, Function<Node, Optional<Version>> nodeToVersionMapper) {
        return nodes.stream()
                .filter(node -> node.type() == nodeType)
                .map(nodeToVersionMapper)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    private static Optional<Version> nodeToCurrentVespaVersion(Node node) {
        return node.status().vespaVersion();
    }

    private static Optional<Version> nodeToWantedVespaVersion(Node node) {
        return node.allocation().map(allocation -> allocation.membership().cluster().vespaVersion());
    }
}
