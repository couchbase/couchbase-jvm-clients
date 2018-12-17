# TODOs

## Service Level

 - add unit tests for the pooling behavior (dynaimc add until limit and remove idle)
 - add integration tests for n1ql which shows the pooling in action

## Supportability

### GC Analyzer

 - figure out which type to set for g1 young and old
 - add tests for the outstanding ones
 - add shenandoah support
 - add zgc support?
 - add azul support?
 - add allocation rate checking

### Diagnostics monitor

 - make diagnostics print in interval debug by default, but if there is a timeout or some
   other weird condition -> print in higher intervals at INFO level so we get it captured.
   
 - make it run in its own thread and include in core env by default on
 - add another analyzer based on the stall detection with latencyUtils. and emit when
   stalls are detected
 - would it be possible to add a analyzer for OS-type stuff like overall memory usage, cpu
   utilization...?
   
### OS Resource Analyzer

 - can we figure out more infos about the system (i.e. cpu usage, memory,...)
 
### JVM Stall Analyzers

 - see if we can use the azul tools to detect stalls and if so report on them in the diagnostics
   report.

## Requests

 - add unit tests for compression both on upsert request and get response
 - rework delay and retry builder (not rely on the reactor one which logs?)
 
## Endpoint

 - add local to endpoint scope if possible, similar to IO
 - add the negotiated hello UUID to the context, if available
 - implement endpoint logic
   (continue testing)
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
 - add encryption/SSL


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

 - make the containerized version work in testing
 - add support for N nodes to bootstrap on managed
 - add annotations for at least version
 - add annotation for specific topology type
 
## Performance tests

 - integrate jmh with gradle
 - start a basic benchmark for the event bus