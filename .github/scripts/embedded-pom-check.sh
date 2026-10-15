#!/bin/sh

# USAGE: Run this script from the repository root after the JARs are built.
#
# The script exits with a non-zero status code if the JAR of a published module
# (one that includes `central-publishing-maven-plugin`) embeds a Couchbase POM
# with unresolved placeholders like `${revision}`, which confuse vulnerability scanners,
# or embeds the POM of a Couchbase artifact other than itself (like a shaded internal module).

FAILED=0

for POM_FILE in */pom.xml; do
  MODULE=$(dirname "$POM_FILE")

  # Skip modules that aren't published. The BOM is published, but it has no JAR.
  if ! grep --quiet "central-publishing-maven-plugin" "$POM_FILE" || [ "$MODULE" = "bom" ]; then
    continue
  fi

  # Find the JAR without a classifier, excluding the pre-shading "original" JAR.
  # shellcheck disable=SC2010
  JAR_FILE=$(ls "$MODULE"/target/*.jar 2>/dev/null | grep --invert-match -e "-sources" -e "-javadoc" -e "/original-")
  if [ "$(echo "$JAR_FILE" | grep --count .)" -ne 1 ]; then
    echo "ERROR: $MODULE: Expected exactly one JAR in $MODULE/target, but found: $JAR_FILE"
    FAILED=1
    continue
  fi

  echo "Checking $JAR_FILE"
  POMS="META-INF/maven/com.couchbase.client/*/pom.xml"

  if [ "$(unzip -Z1 "$JAR_FILE" "$POMS" 2>/dev/null | grep --count .)" -ne 1 ]; then
    echo "ERROR: Expected exactly one embedded Couchbase POM, but found:"
    unzip -Z1 "$JAR_FILE" "$POMS" 2>/dev/null
    FAILED=1
  fi

  # shellcheck disable=SC2016
  if unzip -p "$JAR_FILE" "$POMS" 2>/dev/null | grep --quiet --fixed-strings '${'; then
    echo 'ERROR: Embedded POM has unresolved placeholders'
    FAILED=1
  fi
done

if [ $FAILED -ne 0 ]; then
  echo "Embedded POM check failed. See errors above."
  exit 1
fi

echo "Embedded POM check passed."
