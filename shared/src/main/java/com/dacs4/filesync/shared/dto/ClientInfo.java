package com.dacs4.filesync.shared.dto;

import java.time.Instant;
import java.util.UUID;

public record ClientInfo(UUID sessionId, String name, String remoteAddress, Instant connectedAt) {}
