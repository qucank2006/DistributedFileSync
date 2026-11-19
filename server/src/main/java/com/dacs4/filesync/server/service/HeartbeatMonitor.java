package com.dacs4.filesync.server.service;

import com.dacs4.filesync.server.network.ClientHandler;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class HeartbeatMonitor implements AutoCloseable {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public HeartbeatMonitor(Collection<ClientHandler> connections) {
        scheduler.scheduleWithFixedDelay(() -> {
            long now = System.nanoTime();
            connections.forEach(connection -> connection.expireIfIdle(now));
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
