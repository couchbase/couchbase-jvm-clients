# Changelog

## Java Client

### 3.0.0-dp

## Core IO

### 2.0.0-dp

**New Features**

 - (test) overhauled integration test infrastructure with containerized runner
 - (test) upgraded to junit 5 overall
 - Upgraded to java 8 all around
 - switched from rxjava 1 to reactor for higher performance and best java 8 experience
 - Requests are now based on CompletableFuture for better performance
 - Encoding and Decoding is now in the Request, so adding a new command does touch the
   netty handlers
 - Standalone, efficient, Timer for request timeouts and other parts
 - Each request has a deterministic lifetime and timeout
 - Vastly expanded event bus which now also deals with logging and tracing
 - Overhauled environment with builders and suppliers
 - Endpoints now have built-in circuit breaker functionality
 - Request Context is available on every request and allows to 1) attach custom data and 2) allows to cancel the request from anywhere the context is in scope.