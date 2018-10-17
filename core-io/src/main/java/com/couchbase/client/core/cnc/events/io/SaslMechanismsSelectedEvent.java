package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;
import java.util.Set;

public class SaslMechanismsSelectedEvent extends AbstractEvent {

  private final Set<SaslMechanism> serverMechanisms;
  private final Set<SaslMechanism> allowedMechanisms;
  private final SaslMechanism selectedMechanism;

  public SaslMechanismsSelectedEvent(final IoContext context,
                                     final Set<SaslMechanism> serverMechanisms,
                                     final Set<SaslMechanism> allowedMechanisms,
                                     final SaslMechanism selectedMechanism) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.serverMechanisms = serverMechanisms;
    this.allowedMechanisms = allowedMechanisms;
    this.selectedMechanism = selectedMechanism;
  }

  @Override
  public String description() {
    return "SASL Mechanism " + selectedMechanism
      + " selected. Server: " + serverMechanisms
      + ", Allowed: " + allowedMechanisms;
  }

  public Set<SaslMechanism> serverMechanisms() {
    return serverMechanisms;
  }

  public Set<SaslMechanism> allowedMechanisms() {
    return allowedMechanisms;
  }

  public SaslMechanism selectedMechanism() {
    return selectedMechanism;
  }
}
