#!/bin/sh

# CI script for publishing to Maven Central.

if [ $# -ne 1 ]; then
  echo 1>&2 "$0: requires exactly one argument, the Maven profile (snapshot or release)"
  exit 2
fi

MAVEN_PROFILE=$1

set -e
set -x

./mvnw --batch-mode --file protostellar/pom.xml clean install
./mvnw --batch-mode --file tracing-opentelemetry-deps/pom.xml clean install
./mvnw --batch-mode --file core-io-deps/pom.xml clean install

# Improper shading should have been caught during PR verification, but let's double check.
./mvnw --batch-mode clean install --projects test-utils,core-io,java-client,tracing-opentelemetry -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
cd core-io ; ./shade-check.sh ; cd ..
cd tracing-opentelemetry ; ./shade-check.sh ; cd ..

./mvnw --batch-mode deploy -Dgpg.signer=bc -Dsurefire.rerunFailingTestsCount=1 --activate-profiles ${MAVEN_PROFILE}
./mvnw --batch-mode clean deploy -Dgpg.signer=bc -Dmaven.test.skip=true --activate-profiles ${MAVEN_PROFILE},scala-2.13 --projects scala-implicits,scala-client
./mvnw --batch-mode clean deploy -Dgpg.signer=bc -Dmaven.test.skip=true --activate-profiles ${MAVEN_PROFILE},scala-3 --projects scala-implicits,scala-client
