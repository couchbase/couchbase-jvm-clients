# Kotlin Couchbase client examples

## What's in the box

This module aimed to be a collection of examples of how to use the Couchbase Kotlin SDK in a variety of ways, 
including:
* Cluster, buckets and collections
* Creating, updating, flushing and dropping buckets
* Managing indexes with HttpClient and IndexManager
* Simple data operations with collection and N1QL
* Retrieving data with simple and parametrized queries
* Using LookupInSpec for sub-document search
* Using MutateInSpec for structure update

## Prerequisites

Since this SDK uses Makefile to run the tests, you need to have **Make** installed on your machine. On the first run,
you may also need to execute:

    make deps-only

It will install all the required dependencies for the project.

## Automatic test deployment

All examples are created as integration tests. Most of them have some kind of preparation phase
(@BeforeAll), some of them also have post-cleanup. You can run the examples in automatic mode with:

    mvn clean verify

You need to have a **Docker environment** running on your machine. These examples will start a Couchbase Server,
configure it and run the tests against it. The server will be stopped afterwards.

## Manual test deployment

The Dockerfile inside the repository contains all the required setup to start o Docker container with
a Couchbase server. It replaces standard *entrypoint.sh* with a custom one and automatically creates 
Couchbase cluster with default credentials: 

>Administrator:password

It also creates a bucket named "travel-sample" for simple operations and another bucket "travel-sample-index" 
for index operations. You may also run some custom tests/examples with provided Dockerfile configuration.

## Notes

* There are no default imported data in the configuration, only small imports inside the examples.
* Most of the examples are idempotent, so you can run them multiple times without any side effects.
* Examples use the default scope and collection with only a bucket name specified inside a Keyspace class.

## Troubleshooting

In case of missing dependencies you may need to install **Core IO** and **Kotlin Client SDK** modules manually.
