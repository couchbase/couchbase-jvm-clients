// Want to make this matrix/parameterised, but:
// - Cannot make a proper matrix Jenkins project that's also pipeline
// - Seems to be no way to do e.g. multiple JDKs in a declarative pipeline
// - Can do it with scripted pipeline, but anything inside a node {} block won't trigger the post block, so can't gather junit results
// So, for now, everything is hard-coded.  It's unlikely to change often.
def PLATFORMS = ["ubuntu16"]
def DEFAULT_PLATFORM = PLATFORMS[0]
def platform = DEFAULT_PLATFORM
def LINUX_AGENTS = 'centos6||centos7||ubuntu16||ubuntu14'
def QUICK_TEST_MODE = false // to support quicker development iteration
def ORACLE_JDK = "java"
def ORACLE_JDK_8 = "8u192"
def OPENJDK = "openjdk"
def OPENJDK_8 = "8u202-b08"
def OPENJDK_11 = "11.0.2+7"

pipeline {
    agent { label 'master' }

    options {
        // Safety check, prevent the script running forever
        timeout(time: 60, unit: 'MINUTES')

        // Normally stashes are cleared at the end of the run, but it can be helpful during debugging/development to
        // keep the last stash around (though currently, this workflow doesn't work due to
        // https://issues.jenkins-ci.org/browse/JENKINS-56766)
        preserveStashes(buildCount: 5)
    }

    stages {
        stage('job valid?') {
            when {
                expression {
                    return _INTERNAL_OK_.toBoolean() != true
                }
            }
            steps {
                error("Exiting early as not valid run")
            }
        }

        // Validations are intended to make sure that the commit is sane.  Things like code-formatting rules and basic
        // sanity tests go here.
        stage('prepare and validate') {
            agent { label LINUX_AGENTS }
            steps {
                cleanWs()
                dir('couchbase-jvm-clients') {
                    checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO', poll: false]]])
                }
                stash includes: 'couchbase-jvm-clients/', name: 'couchbase-jvm-clients', useDefaultExcludes: false
            }
        }

        stage('build') {
            // agent { label LINUX_AGENTS }
            // Hit a random scalafmt error when running on other Linux platforms...
            agent { label DEFAULT_PLATFORM }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            steps {
                // Is cleanWs strictly needed?  Probably not, but hit odd AccessDeniedException errors without it...
                // Update: and 'stale source detected' errors - just, always safer to clean then unstash
                cleanWs()
                unstash 'couchbase-jvm-clients'
                installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)

                dir('couchbase-jvm-clients') {
                    shWithEcho("echo $JAVA_HOME")
                    shWithEcho("ls $JAVA_HOME")
                    shWithEcho("echo $PATH")
                    shWithEcho("java -version")
                    shWithEcho("make deps-only")

                    // Skips the tests, that's done in other stages
                    // The -B -Dorg... stuff hides download progress messages, very verbose
                    shWithEcho("mvn install -Dmaven.test.skip -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")

                    // This is to speed up iteration during development, skips out some stuff
                    // shWithEcho("mvn -pl '!scala-client,!scala-implicits,!benchmarks' install -Dmaven.test.skip -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")
                }

                stash includes: 'couchbase-jvm-clients/', name: 'couchbase-jvm-clients', useDefaultExcludes: false
            }
        }

        // Test against mock - this skips a lot of tests, and is intended for quick validation
        stage('validation testing (mock, Oracle JDK 8)') {
            agent { label LINUX_AGENTS }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == true }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)

                    dir('couchbase-jvm-clients') {
                        // By default Java and Scala use mock for testing
                        shWithEcho("mvn --fail-at-end test")

                        // While iterating Jenkins development, this makes it much faster:
                        // shWithEcho("mvn package surefire:test -Dtest=com.couchbase.client.java.ObserveIntegrationTest -pl java-client")
                    }
                }
            }
            post {
                always {
                    // Process the Junit test results
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
                failure { emailFailure() }
                success { emailSuccess() }
            }
        }

        // Test against cbdyncluster - do for nightly tests
        // One day can get all these cbdyncluster tests running in parallel: https://jenkins.io/blog/2017/09/25/declarative-1/
        stage('testing  (Linux, cbdyncluster 6.5, Oracle JDK 8) ') {
            agent { label 'sdk-integration-test-linux' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)
                    dir('couchbase-jvm-clients') {
                        script { testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
                failure { emailFailure() }
                success { emailSuccess() }
            }
        }

        stage('testing  (Linux, cbdyncluster 6.0.3, Oracle JDK 8) ') {
            agent { label 'sdk-integration-test-linux' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)
                    dir('couchbase-jvm-clients') {
                        script { testAgainstServer("6.0.3", QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
                failure { emailFailure() }
                success { emailSuccess() }
            }
        }

        stage('testing  (Linux, cbdyncluster 5.5.5, Oracle JDK 8) ') {
            agent { label 'sdk-integration-test-linux' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)
                    dir('couchbase-jvm-clients') {
                        script { testAgainstServer("5.5.5", QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
                failure { emailFailure() }
                success { emailSuccess() }
            }
        }

        // Someone smarter than I could work out how to parameterise linux & windows testing without C&P...
        stage('testing (Windows, cbdyncluster 6.5, Oracle JDK 8) ') {
            agent { label 'sdk-integration-test-windows' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded("windows", ORACLE_JDK, ORACLE_JDK_8)

                    dir('couchbase-jvm-clients') {
                        script {
                            testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE)
                        }
                    }
                }
            }
            post {
                always {
                    // Process the Junit test results
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (Linux, cbdyncluster 6.5, AdoptOpenJDK 11) ') {
            agent { label 'sdk-integration-test-linux' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_11)
                    dir('couchbase-jvm-clients') {
                        script { testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
                failure { emailFailure() }
                success { emailSuccess() }
            }
        }

        stage('testing (Linux, cbdyncluster 6.5, AdoptOpenJDK 8) ') {
            agent { label 'sdk-integration-test-linux' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_8)
                    dir('couchbase-jvm-clients') {
                        script { testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
                failure { emailFailure() }
                success { emailSuccess() }
            }
        }

        stage('package') {
            steps {
                cleanWs()
                unstash 'couchbase-jvm-clients'

                dir('couchbase-jvm-clients') {
                    // archiveArtifacts artifacts: 'couchbase-jvm-clients/', fingerprint: true
                    archiveArtifacts artifacts: 'java-client/build/libs/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'scala-client/build/libs/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'core-io/build/libs/*.jar', fingerprint: true
                    archiveArtifacts artifacts: '**/pom.xml', fingerprint: true
                }
            }
        }
    }
}

void emailSuccess() {
    def emailAddress = 'graham.pople@couchbase.com'
    mail to: emailAddress,
            subject: "Successful Pipeline: ${currentBuild.fullDisplayName}",
            body: "Succeeded: ${env.BUILD_URL}"
}

void emailFailure() {
    def emailAddress = 'graham.pople@couchbase.com'
    mail to: emailAddress,
            subject: "Failed Pipeline: ${currentBuild.fullDisplayName}",
            body: "Something is wrong with ${env.BUILD_URL}"
}

void shWithEcho(String command) {
    if (NODE_NAME.contains("windows")) {
        echo bat(script: command, returnStdout: true)
    } else {
        echo sh(script: command, returnStdout: true)
    }
}

// Installs JDK to the workspace using cbdep tool
String installJDKIfNeeded(platform, javaPackage, javaVersion) {
    def install = false

    echo "checking install"
    if (!fileExists("deps")) {
        echo "file deps does not exist"
        install = true
    } else {
        echo "file deps does exist"
        dir("deps") {
            install = !fileExists("$javaPackage-${javaVersion}")
            if (install) {
                echo "$javaPackage-${javaVersion} exists"
            } else {
                echo "$javaPackage-${javaVersion} does not exist"
            }
        }
    }

    if (install) {
        shWithEcho("mkdir -p deps && mkdir -p deps/$javaPackage-${javaVersion}")
        shWithEcho("cbdep install -d deps $javaPackage ${javaVersion}")
    }
}

void createIntegrationTestPropertiesFile(String filename, String ip) {
    shWithEcho("echo 'cluster.type=unmanaged' > ${filename}")
    shWithEcho("echo 'cluster.adminUsername=Administrator' >> ${filename}")
    shWithEcho("echo 'cluster.adminPassword=password' >> ${filename}")
    shWithEcho("echo 'cluster.unmanaged.seed=${ip}:8091' >> ${filename}")
    shWithEcho("echo 'cluster.unmanaged.numReplicas=0' >> ${filename}")

    shWithEcho("cat ${filename}")

}

// To be called inside a script {} block - required so can do try-finally logic to cleanup the cbdyncluster
// (Inside a script {} block is 'scripted pipeline' syntax, different to the 'declarative pipeline' syntax elsewhere.)
void testAgainstServer(String serverVersion, boolean QUICK_TEST_MODE) {
    def clusterId = null
    try {
        // For debugging
        shWithEcho("echo $JAVA_HOME")
        shWithEcho("ls $JAVA_HOME")
        shWithEcho("echo $PATH")
        shWithEcho("java -version")

        // For debugging, what clusters are open
        shWithEcho("cbdyncluster ps -a")

        // May need to remove some manually if they're stuck.  -f forces, allows deleting cluster we didn't open
        // shWithEcho("cbdyncluster rm -f 3d023261")

        // Allocate the cluster
        clusterId = sh(script: "cbdyncluster allocate --num-nodes=3 --server-version=" + serverVersion, returnStdout: true)
        echo "Got cluster ID $clusterId"

        // Find the cluster IP
        def ips = sh(script: "cbdyncluster ips $clusterId", returnStdout: true).trim()
        echo "Got raw cluster IPs " + ips
        def ip = ips.tokenize(',')[0]
        echo "Got cluster IP http://" + ip + ":8091"

        // By default Java and Scala use mock for testing, make them use cbdyncluster instead
        createIntegrationTestPropertiesFile('java-client/src/integrationTest/resources/integration.properties', ip)
        createIntegrationTestPropertiesFile('scala-client/src/integrationTest/resources/integration.properties', ip)

        // Create the cluster
        if (!QUICK_TEST_MODE) {
            shWithEcho("cbdyncluster --node kv,index,n1ql,fts,cbas --node kv --node kv --bucket default setup $clusterId")
        } else {
            // During development, this is faster (less tests)
            shWithEcho("cbdyncluster --node kv --node kv --node kv --bucket default setup $clusterId")
        }

        // Make the bucket flushable
        shWithEcho("curl -v -X POST -u Administrator:password -d flushEnabled=1 http://" + ip + ":8091/pools/default/buckets/default")

        // Not sure why this is needed, it should be in stash from build....
        shWithEcho("make deps-only")

        // The -B -Dorg... stuff hides download progress messages, very verbose
        if (!QUICK_TEST_MODE) {
            // Removing scala for now
            shWithEcho("mvn -pl '!scala-client,!scala-implicits,!benchmarks' --fail-at-end install test -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")
        } else {
            // This is for iteration during development, skips out some steps
            shWithEcho("mvn -pl '!scala-client,!scala-implicits,!benchmarks' --fail-at-end install test -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")

            // While iterating Jenkins development, this makes it much faster:
            // shWithEcho("mvn package surefire:test -Dtest=com.couchbase.client.java.ObserveIntegrationTest -pl java-client")
        }

    }
    finally {
        if (clusterId != null) {
            // Easy to run out of resources during iterating, so cleanup even
            // though cluster will be auto-removed after a time

            sh(script: "cbdyncluster rm $clusterId")
        }
    }
}