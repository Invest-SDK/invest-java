package ru.ttech.piapi.core.impl.marketdata.subscription;

import lombok.Getter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionEntry {
  @Getter
  private final Instrument instrument;
  private final AtomicReference<LocalDateTime> lastReceiveTime = new AtomicReference<>
    (Instant.now().atOffset(ZoneOffset.UTC)
      .toLocalDateTime().truncatedTo(ChronoUnit.SECONDS));
  private final AtomicBoolean isActive = new AtomicBoolean(true);

  public SubscriptionEntry(Instrument instrument) {
    this.instrument = instrument;
  }

  public LocalDateTime getLastReceiveTime() {
    return lastReceiveTime.get();
  }

  public void updateLastUpdateTime(LocalDateTime time) {
    lastReceiveTime.set(time);
  }

  public boolean subscriptionEquals(Instrument other) {
    return instrument.subscriptionEquals(other);
  }

  public boolean isActive() {
    return isActive.get();
  }

  public void setActive(boolean active) {
    isActive.set(active);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SubscriptionEntry that)) return false;
    return instrument.subscriptionEquals(that.instrument);
  }

  @Override
  public int hashCode() {
    return instrument.getInstrumentId().hashCode();
  }
}

