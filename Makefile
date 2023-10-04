install: deps-only
	@./mvnw install

deps-only:
	@./mvnw --batch-mode -f protostellar/pom.xml clean install
	@./mvnw --batch-mode -f core-io-deps/pom.xml clean install
	@./mvnw --batch-mode -f tracing-opentelemetry-deps/pom.xml clean install
