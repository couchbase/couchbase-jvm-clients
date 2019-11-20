install:
	@mvn -f core-io-deps/pom.xml clean install
	@mvn install

deps-only:
	@mvn -f core-io-deps/pom.xml clean install -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
