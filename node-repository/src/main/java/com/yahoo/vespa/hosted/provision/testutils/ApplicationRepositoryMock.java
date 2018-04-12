package com.yahoo.vespa.hosted.provision.testutils;

import com.yahoo.vespa.config.server.ApplicationRepository;

import java.time.Clock;

public class ApplicationRepositoryMock extends ApplicationRepository {
    public ApplicationRepositoryMock() {
        super(null, null, Clock.systemUTC());
    }
}
