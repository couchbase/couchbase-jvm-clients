# Benchmarks
Holds any benchmarks relevant to the clients or core.  


Run with `./gradlew jmh`

## Why not JMH?
JMH on gradle relies on a plugin.  Hit a lot of random compilation issues that seemed to involve the gradle cache and haven't been fixed despite
being open for years:

https://github.com/melix/jmh-gradle-plugin/issues/89
https://github.com/melix/jmh-gradle-plugin/issues/90

None of the workarounds worked, so trying ScalaMeter instead.