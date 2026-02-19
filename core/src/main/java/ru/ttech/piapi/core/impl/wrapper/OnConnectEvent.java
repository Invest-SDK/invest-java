package ru.ttech.piapi.core.impl.wrapper;

import lombok.Builder;

import java.util.UUID;

@Builder
public record OnConnectEvent(UUID id) {
}
