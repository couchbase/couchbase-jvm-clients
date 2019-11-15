install:
	@mvn -f core-io-deps/pom.xml clean install
	@mvn install

deps-only:
	@mvn -f core-io-deps/pom.xml clean install
