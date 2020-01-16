// Want to make this matrix/parameterised, but:
// - Cannot make a proper matrix Jenkins project that's also pipeline
// - Seems to be no way to do e.g. multiple JDKs in a declarative pipeline
// - Can do it with scripted pipeline, but anything inside a node {} block won't trigger the post block, so can't gather junit results
// So, for now, everything is hard-coded.  It's unlikely to change often.
// TODO: stashing the junit file after its generated and unstashing it in post, may work
def PLATFORMS = ["ubuntu16"]
def DEFAULT_PLATFORM = PLATFORMS[0]
def platform = DEFAULT_PLATFORM
def LINUX_AGENTS = 'centos6||centos7||ubuntu16||ubuntu14'
def QUICK_TEST_MODE = false // enable to support quicker development iteration

// Java versions available through cbdeps are on
// https://hub.internal.couchbase.com/confluence/pages/viewpage.action?spaceKey=CR&title=cbdep+available+packages
def ORACLE_JDK = "java"
def ORACLE_JDK_8 = "8u192"
def OPENJDK = "openjdk"
def OPENJDK_8 = "8u202-b08"
def OPENJDK_11 = "11.0.2+7"
def CORRETTO = "corretto"
def CORRETTO_8 = "8.232.09.1"
def CORRETTO_11 = "11.0.4.11-1"

// The lucky spammees
EMAILS = ['graham.pople@couchbase.com', 'michael.nitschinger@couchbase.com', 'david.kelly@couchbase.com', 'david.nault@couchbase.com']

pipeline {
    agent { label 'master' }

    options {
        // Safety check, prevent the script running forever
        timeout(time: 300, unit: 'MINUTES')

        // Normally stashes are cleared at the end of the run, but it can be helpful during debugging/development to
        // keep the last stash around (though currently, this workflow doesn't work due to
        // https://issues.jenkins-ci.org/browse/JENKINS-56766)
        preserveStashes(buildCount: 5)
    }

    stages {
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

        stage('build Oracle 8') {
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
                    // shWithEcho("mvn -pl '!scala-client,!scala-implicits' install -Dmaven.test.skip -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")
                }

                stash includes: 'couchbase-jvm-clients/', name: 'couchbase-jvm-clients', useDefaultExcludes: false
            }
        }

        stage('build OpenJDK 11') {
            agent { label DEFAULT_PLATFORM }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}/bin:$PATH"
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_11)

                    dir('couchbase-jvm-clients') {
                        shWithEcho("make deps-only")
                        shWithEcho("mvn install -Dmaven.test.skip -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")
                    }
                }
            }
        }

        // Scala 2.11 & 2.13 aren't officially distributed or supported, but we have community depending on it so check
        // they at least compile
        stage('build Scala 2.11') {
            agent { label DEFAULT_PLATFORM }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}/bin:$PATH"
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    // 2.11 must be built with JDK 8
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_8)

                    dir('couchbase-jvm-clients') {
                        shWithEcho("make deps-only")
                        shWithEcho("mvn -Dmaven.test.skip -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Dscala.compat.version=2.11 -Dscala.compat.library.version=2.11.12 clean compile")
                    }
                }
            }
        }

        stage('build Scala 2.13') {
            agent { label DEFAULT_PLATFORM }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}/bin:$PATH"
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanWs()
                    unstash 'couchbase-jvm-clients'
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_11)

                    dir('couchbase-jvm-clients') {
                        shWithEcho("make deps-only")
                        shWithEcho("mvn -Dmaven.test.skip -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Dscala.compat.version=2.13 -Dscala.compat.library.version=2.13.1 clean compile")
                    }
                }
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
            }
        }

        // Test against cbdyncluster - do for nightly tests
        // One day can get all these cbdyncluster tests running in parallel: https://jenkins.io/blog/2017/09/25/declarative-1/
        stage('testing  (Linux, cbdyncluster 6.5, Oracle JDK 8)') {
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
            }
        }

         stage('testing  (Linux, cbdyncluster 6.0.3, Oracle JDK 8)') {
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
             }
         }

         stage('testing  (Linux, cbdyncluster 5.5.5, Oracle JDK 8)') {
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
                         script { testAgainstServer("5.5.5", QUICK_TEST_MODE, false) }
                     }
                 }
             }
             post {
                 always {
                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                 }
             }
         }

//        stage('testing  (Linux, cbdyncluster 5.1.3, Oracle JDK 8)') {
//            agent { label 'sdk-integration-test-linux' }
//            environment {
//                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
//                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
//            }
//            when {
//                expression
//                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
//            }
//            steps {
//                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
//                    cleanWs()
//                    unstash 'couchbase-jvm-clients'
//                    installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)
//                    dir('couchbase-jvm-clients') {
//                        script { testAgainstServer("5.1.3", QUICK_TEST_MODE, false) }
//                    }
//                }
//            }
//            post {
//                always {
//                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                }
//            }
//        }

        stage('testing (Linux, cbdyncluster 6.5, AdoptOpenJDK 11)') {
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
             }
         }

         stage('testing (Linux, cbdyncluster 6.5, AdoptOpenJDK 8)') {
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
             }
         }

        // Commented out until the failing tests are reduced
//        stage('testing (Linux, cbdyncluster 6.5, Amazon Corretto 8)') {
//            agent { label 'sdk-integration-test-linux' }
//            environment {
//                JAVA_HOME = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_8}"
//                PATH = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_8}/bin:$PATH"
//            }
//            when {
//                expression
//                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
//            }
//            steps {
//                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
//                    cleanWs()
//                    unstash 'couchbase-jvm-clients'
//                    installJDKIfNeeded(platform, CORRETTO, CORRETTO_8)
//                    dir('couchbase-jvm-clients') {
//                        script { testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE) }
//                    }
//                }
//            }
//            post {
//                always {
//                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                }
//            }
//        }

        //        stage('testing (Linux, cbdyncluster 6.5, Amazon Corretto 11)') {
//            agent { label 'sdk-integration-test-linux' }
//            environment {
//                JAVA_HOME = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_11}"
//                PATH = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_11}/bin:$PATH"
//            }
//            when {
//                expression
//                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
//            }
//            steps {
//                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
//                    cleanWs()
//                    unstash 'couchbase-jvm-clients'
//                    installJDKIfNeeded(platform, CORRETTO, CORRETTO_11)
//                    dir('couchbase-jvm-clients') {
//                        script { testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE) }
//                    }
//                }
//            }
//            post {
//                always {
//                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                }
//            }
//        }

        // Commented for now as sdk-integration-test-win temporarily down
//         stage('testing (Windows, cbdyncluster 6.5, Oracle JDK 8)') {
//             agent { label 'sdk-integration-test-win' }
//             environment {
//                 JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
//                 PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
//             }
//             when {
//                 expression
//                         { return IS_GERRIT_TRIGGER.toBoolean() == false }
//             }
//             steps {
//                 catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
//                     cleanWs()
//                     unstash 'couchbase-jvm-clients'
//                     installJDKIfNeeded("windows", ORACLE_JDK, ORACLE_JDK_8)
//
//                     dir('couchbase-jvm-clients') {
//                         script {
//                             testAgainstServer(SERVER_TEST_VERSION, QUICK_TEST_MODE)
//                         }
//                     }
//                 }
//             }
//             post {
//                 always {
//                     // Process the Junit test results
//                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                 }
//             }
//         }

        stage('package') {
            steps {
                cleanWs()
                unstash 'couchbase-jvm-clients'

                dir('couchbase-jvm-clients') {
                    shWithEcho("find . -iname *.jar")
                    // archiveArtifacts artifacts: 'couchbase-jvm-clients/', fingerprint: true
                    archiveArtifacts artifacts: 'java-client/target/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'scala-client/target/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'core-io/target/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'java-examples/target/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'tracing-opentelemetry/target/*.jar', fingerprint: true
                    archiveArtifacts artifacts: 'tracing-opentracing/target/*.jar', fingerprint: true
                    archiveArtifacts artifacts: '**/pom.xml', fingerprint: true
                }
            }
        }
    }
    post {
        failure { emailFailure() }
        success { emailSuccess() }
    }
}

void emailSuccess() {
    EMAILS.each {
        def email = it
        mail to: email,
                subject: "Successful Pipeline: ${currentBuild.fullDisplayName}",
                body: "Succeeded: ${env.BUILD_URL}"
    }
}

void emailFailure() {
    EMAILS.each {
        def email = it
        mail to: email,
                subject: "Failed Pipeline: ${currentBuild.fullDisplayName}",
                body: "Something is wrong with ${env.BUILD_URL}: ${env.FAILED_TESTS}"
    }
}

void shWithEcho(String command) {
    if (NODE_NAME.contains("windows")) {
        echo bat(script: command, returnStdout: true)
    } else {
        echo sh(script: command, returnStdout: true)
    }
}

void shIgnoreFailure(String command) {
    if (NODE_NAME.contains("windows")) {
        bat(script: command, returnStatus: true)
    } else {
        sh(script: command, returnStatus: true)
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
void testAgainstServer(String serverVersion, boolean QUICK_TEST_MODE, boolean includeAnalytics = true) {
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
            if (includeAnalytics) {
                shWithEcho("cbdyncluster --node kv,index,n1ql,fts,cbas --node kv --node kv --bucket default setup $clusterId")
            }
            else {
                shWithEcho("cbdyncluster --node kv,index,n1ql,fts --node kv --node kv --bucket default setup $clusterId")
            }
        } else {
            // During development, this is faster (less tests)
            shWithEcho("cbdyncluster --node kv --node kv --node kv --bucket default setup $clusterId")
        }

        // Make the bucket flushable
        shWithEcho("curl -v -X POST -u Administrator:password -d flushEnabled=1 http://" + ip + ":8091/pools/default/buckets/default")

        // Set the query indexer mode.  Without this query tests fail with "GSI CreatePrimaryIndex() - cause: Please Set Indexer Storage Mode Before Create Index"
        shWithEcho("curl -v -X POST -u Administrator:password -d 'storageMode=plasma' http://" + ip + ":8091/settings/indexes")

        // Not sure why this is needed, it should be in stash from build....
        shWithEcho("make deps-only")

        // The -B -Dorg... stuff hides download progress messages, very verbose
        if (!QUICK_TEST_MODE) {
            shWithEcho("mvn --fail-at-end install -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")
        } else {
            // This is for iteration during development, skips out some steps
            shWithEcho("mvn -pl '!scala-client,!scala-implicits' --fail-at-end install test -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")

            // Another iteration option, this runs just one test
            //shWithEcho("mvn package surefire:test -Dtest=com.couchbase.client.java.ObserveIntegrationTest -pl java-client")
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