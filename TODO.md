# TODOs

Loosely keeping track of things:

## IO

 - complete the select bucket code
 - complete select bucket handler test case

 - complete sasl handler code 
 - add sasl handler unit tests for all mechs
 - add sasl client unit tests
 
 - get a full kv pipeline working

## Env
 
 - allow to configure the env through certain config providers
 - split up the env into multiple sub-sections (like IO)
 - make the UserAgent an actual object with parts and a toString
 
## Event Bus

 - add filtering mechanisms for the consumer
 - add "measure" every N invocations and if the queue fills up report the latency for slow consumers. threshold?
 
## Logging

 - figure out a proper logger setup for tests
 - add log redaction functionality
 

## Integration tests

 - Once a basic kv pipeline is up, integrate with testcontainers-java and see how that works against a matrix of versions
 
## Performance tests

 - integrate jmh with gradle
 - start a basic benchmark for the event bus