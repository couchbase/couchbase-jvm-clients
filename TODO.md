# TODOs

Loosely keeping track of things:


## GC Analyzer

 - figure out which type to set for g1 young and old
 - add tests for the outstanding ones
 - add shenandoah support
 - add zgc support?
 - add azul support?
 - add allocation rate checking

## Diagnostics monitor

 - make it run in its own thread and include in core env by default on
 - add another analyzer based on the stall detection with latencyUtils. and emit when
   stalls are detected
 - would it be possible to add a analyzer for OS-type stuff like overall memory usage, cpu
   utilization...?

## Requests

 - add unit tests for compression both on upsert request and get response
 - rework delay and retry builder (not rely on the reactor one which logs?)
 - figure out a retry policy per request and use it
 
## Endpoint

 - add local to endpoint scope if possible, similar to IO
 - add the negotiated hello UUID to the context, if available
 - implement endpoint logic
   (continue testing)
 - implement circuit breaker logic
 - test it all in unit tests
 - add integration test for circuit breaker (?)

## IO

 - figure out a way if we can support event loop separation for "high priority"
   requests?
 - add the negotiated hello UUID to the context, if available
 - add test coverage for the ever-incrementing opaque functionality
 - complete select bucket handler test case with error cases
 - add sasl handler unit tests for all mechs
 - add sasl client unit tests
 - add collection support with tests
 - add snappy support and then reenable

## Env
 
 - initialize a couchbase scheduler for reactor
 - use the reactor scheduler everyhwere we need it
 
 - make sure all changeable properties have suppliers!
 - allow to configure the env through certain config providers
 - make the UserAgent an actual object with parts and a toString
 - suppliers should be cacheable somehow
 
## Event Bus

 - add filtering mechanisms for the consumer
 - add "measure" every N invocations and if the queue fills up report the 
   latency for slow consumers. threshold?
 
## Logging

 - add log redaction functionality
 - add functionality that if a payload is attached to a request context AND enabled
    via config that it is put int he thread local store for the logger...
    
## Integration tests

 - add support for the mock! as another managed cluster
 - Finish work to get an initial managed integration build going
 - add support for N nodes to bootstrap on managed
 - add annotations for at least version
 - add annotation for specific topology type
 
## Performance tests

 - integrate jmh with gradle
 - start a basic benchmark for the event bus