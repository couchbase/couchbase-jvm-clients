# TODOs

Loosely keeping track of things:

## IO

 - make sure each channel for kv handles the opaque also for the full bootstrap chain! (pass around and then use?)
 - complete select bucket handler test case with error cases
 - add sasl handler unit tests for all mechs
 - add sasl client unit tests
 - add collection support with tests
 
 - get a full kv pipeline working

## Env
 
 - allow to configure the env through certain config providers
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