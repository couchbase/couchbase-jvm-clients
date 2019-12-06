install:
	@./mvnw -f core-io-deps/pom.xml clean install
	@./mvnw install

deps-only:
	@./mvnw -f core-io-deps/pom.xml clean install -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
