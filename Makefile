MVN = docker run --rm -v $(PWD):/w -v tb-m2:/root/.m2 -w /w/flink-job maven:3.10.0-eclipse-temurin-17-noble mvn -B

.PHONY: jar test-java

jar:
	$(MVN) package

test-java:
	$(MVN) verify
