#!/bin/sh

# USAGE: Run this script from the `core-io` directory after the JAR is built.
#
# The script exits with a non-zero status code if the core-io JAR
# contains files from a library that was not shaded correctly.

set -e

# Find the JAR without a "sources" or "javadoc" classifier
JAR_FILE=$(ls target/core-io-*.jar | grep --invert-match -e "sources" -e "javadoc")

# Fail if there's a problem with how the script got the filename.
# (Like, maybe there's an old snapshot getting in the way.)
#
# Use archaic option format for compatibility with Java 8.
#   t is --list (List the table of contents for the archive)
#   f is --file (Read from a file)
jar tf "$JAR_FILE" > /dev/null

echo "Scanning $JAR_FILE for unexpected packages or native libraries..."

# We *want* grep to exit with a status code of 1 (nothing matched).
set +e

# Scan for unexpected packages.
# Line ends with "/" == just a directory; don't care about it.
# Line starts with "com/couchbase/" == it's our code, or something we've relocated.
# Line starts with "couchbase/" == protostellar proto definitions.
# Anything else is unexpected.
jar tf "$JAR_FILE" | grep --invert-match -e "/$" -e "META-INF" -e "^com/couchbase/" -e "^couchbase/"
if [ $? -ne 1 ]; then
  echo "ERROR: Found unexpected packages in $JAR_FILE (see above)"
  echo "Please exclude or relocate them, or update the shade-check.sh script to expect them."
  exit 1
fi

# Scan for unexpected native libraries.
jar tf "$JAR_FILE" | grep "META-INF/native/." | grep --invert-match -e "/$" -e "com_couchbase_client_core_deps_"
if [ $? -ne 1 ]; then
  echo "ERROR: Found unexpected native libraries in $JAR_FILE (see above)"
  echo "Please exclude or relocate them, or update the shade-check.sh script to expect them."
  exit 1
fi

echo "Scan complete. No un-relocated shaded libraries found. That's good!"
