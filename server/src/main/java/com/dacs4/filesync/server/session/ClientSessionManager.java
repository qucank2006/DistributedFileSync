package com.dacs4.filesync.server.session;

import com.dacs4.filesync.shared.dto.ClientInfo;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ClientSessionManager {
    private final ConcurrentMap<UUID, ClientInfo> sessions = new ConcurrentHashMap<>();

    public ClientInfo register(String name, String remoteAddress) {
        ClientInfo client;
        do {
            client = new ClientInfo(UUID.randomUUID(), name, remoteAddress, Instant.now());
        } while (sessions.putIfAbsent(client.sessionId(), client) != null);
        return client;
    }

    public void remove(ClientInfo client) {
        sessions.remove(client.sessionId(), client);
    }
}
