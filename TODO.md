# TODOs

Loosely keeping track of things:

## IO

 - in all the kv bootstrap handlers, add a sanity check method to make sure we are getting a good packet (and i.e. not chunked)
 - complete select bucket handler test cases
 - add sasl in the pipeline
 - get a full kv pipeline working

 
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