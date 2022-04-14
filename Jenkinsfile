// Want to make this matrix/parameterised, but:
// - Cannot make a proper matrix Jenkins project that's also pipeline
// - Seems to be no way to do e.g. multiple JDKs in a declarative pipeline
// - Can do it with scripted pipeline, but anything inside a node {} block won't trigger the post block, so can't gather junit results
// So, for now, everything is hard-coded.  It's unlikely to change often.
// TODO: stashing the junit file after its generated and unstashing it in post, may work
PLATFORMS = ["ubuntu16", "ubuntu20"]
DEFAULT_PLATFORM = PLATFORMS[0]
platform = DEFAULT_PLATFORM
LINUX_AGENTS = 'centos6||centos7||ubuntu16||ubuntu14||ubuntu20'
QUICK_TEST_MODE = false // enable to support quicker development iteration

// Java versions available through cbdeps are on
// https://hub.internal.couchbase.com/confluence/pages/viewpage.action?spaceKey=CR&title=cbdep+available+packages
// https://github.com/couchbasebuild/cbdep/blob/master/cbdep.config
def ORACLE_JDK = "java"
def ORACLE_JDK_8 = "8u192"
def OPENJDK = "openjdk"
def OPENJDK_8 = "8u202-b08"
def OPENJDK_11 = "11.0.2+7"
def OPENJDK_11_M1 = "11.0.11+9"
def OPENJDK_17 = "17.0.1+12"
def CORRETTO = "corretto"         // Amazon JDK
def CORRETTO_8 = "8.232.09.1"     // available versions: https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/doc-history.html
def CORRETTO_11 = "11.0.5.10.1"   // available versions: https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/doc-history.html

// The lucky spammees
EMAILS = ['graham.pople@couchbase.com', 'michael.nitschinger@couchbase.com', 'david.kelly@couchbase.com', 'david.nault@couchbase.com']

pipeline {
    agent none

    options {
        // Safety check, prevent the script running forever
        timeout(time: 300, unit: 'MINUTES')
    }

    stages {
        // Scala 2.11 & 2.13 aren't officially distributed or supported, but we have community depending on it so check
        // they at least compile
        stage('build Scala 2.11') {
            agent { label DEFAULT_PLATFORM }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}/bin:$PATH"
            }
            steps {
                // 2.11 must be built with JDK 8
                buildScala(OPENJDK, OPENJDK_8, "2.11", "2.11.12")
            }
        }

        stage('build Scala 2.13') {
            agent { label DEFAULT_PLATFORM }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}/bin:$PATH"
            }
            steps {
                buildScala(OPENJDK, OPENJDK_11, "2.13", "2.13.7")
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
                    cleanupWorkspace()
                    installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)

                    dir('couchbase-jvm-clients') {
                        checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])
                        // By default Java and Scala use mock for testing
                        shWithEcho("mvn --fail-at-end clean test")

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

        // JDK combination tests

        stage('testing (Linux, cbdyncluster 7.0-release, openjdk 17)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_17}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_17}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(OPENJDK, OPENJDK_17, "7.0-release")
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (Linux, cbdyncluster 6.5, Amazon Corretto 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_8}"
                PATH = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(CORRETTO, CORRETTO_8, "6.5-release")
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (Linux, cbdyncluster 6.5, Amazon Corretto 11)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_11}"
                PATH = "${WORKSPACE}/deps/${CORRETTO}-${CORRETTO_11}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(CORRETTO, CORRETTO_11, "6.5-release")
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (Linux, cbdyncluster 6.5, AdoptOpenJDK 11)') {
             agent { label 'sdkqe-centos7' }
             environment {
                 JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}"
                 PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}/bin:$PATH"
             }
             when {
                 expression
                         { return IS_GERRIT_TRIGGER.toBoolean() == false }
             }
             steps {
                test(OPENJDK, OPENJDK_11, "6.5-release")
             }
             post {
                 always {
                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                 }
             }
         }

         stage('testing (Linux, cbdyncluster 6.5, AdoptOpenJDK 8)') {
             agent { label 'sdkqe-centos7' }
             environment {
                 JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}"
                 PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_8}/bin:$PATH"
             }
             when {
                 expression
                         { return IS_GERRIT_TRIGGER.toBoolean() == false }
             }
             steps {
                 test(OPENJDK, OPENJDK_8, "6.5-release")
             }
             post {
                 always {
                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                 }
             }
         }

        //Server combination tests

        // Test against cbdyncluster - do for nightly tests
        // One day can get all these cbdyncluster tests running in parallel: https://jenkins.io/blog/2017/09/25/declarative-1/
        stage('testing (Linux, cbdyncluster 6.5-release, colossus, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/colossus/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/colossus/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('colossus') {
                        checkout([$class: 'GitSCM',
                                  branches: [[name: 'colossus']],
                                  userRemoteConfigs: [[url: '$REPO']]])
                        installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)
                        script { testAgainstServer("6.5-release", QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing  (Linux, cbdyncluster 6.5-release, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(ORACLE_JDK, ORACLE_JDK_8, "6.5-release")
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing  (Linux, cbdyncluster 6.0-release, Oracle JDK 8)') {
             agent { label 'sdkqe-centos7' }
             environment {
                 JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                 PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
             }
             when {
                 expression
                         { return IS_GERRIT_TRIGGER.toBoolean() == false }
             }
             steps {
                 test(ORACLE_JDK, ORACLE_JDK_8, "6.0-release")
             }
             post {
                 always {
                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                 }
             }
         }

        stage('testing  (Linux, cbdyncluster 5.5-release, Oracle JDK 8)') {
             agent { label 'sdkqe-centos7' }
             environment {
                 JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                 PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
             }
             when {
                 expression
                         { return IS_GERRIT_TRIGGER.toBoolean() == false }
             }
             steps {
                 test(ORACLE_JDK, ORACLE_JDK_8, "5.5-release", includeAnalytics : false)
             }
             post {
                 always {
                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                 }
             }
         }

        stage('testing  (Linux, cbdyncluster 6.6-release, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(ORACLE_JDK, ORACLE_JDK_8, "6.6-release")
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (Linux, cbdyncluster 6.6-release, colossus, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/colossus/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/colossus/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('colossus') {
                        checkout([$class: 'GitSCM',
                                  branches: [[name: 'colossus']],
                                  userRemoteConfigs: [[url: '$REPO']]])
                        installJDKIfNeeded(platform, ORACLE_JDK, ORACLE_JDK_8)
                        script { testAgainstServer("6.6-release", QUICK_TEST_MODE) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing  (Linux, cbdyncluster 7.0-release, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(ORACLE_JDK, ORACLE_JDK_8, "7.0-release", includeEventing : true)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing  (Linux, cbdyncluster 7.1-stable, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(ORACLE_JDK, ORACLE_JDK_8, "7.1-stable", includeEventing : true)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing  (Linux, cbdyncluster 7.1-stable with multi certs, Oracle JDK 8)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(ORACLE_JDK, ORACLE_JDK_8, "7.1-stable", includeEventing : true, multiCerts : true)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing  (Linux, cbdyncluster 7.0-release, Oracle JDK 8, CE)') {
            agent { label 'sdkqe-centos7' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}"
                PATH = "${WORKSPACE}/deps/${ORACLE_JDK}-${ORACLE_JDK_8}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(ORACLE_JDK, ORACLE_JDK_8, "7.0-release", ceMode : true)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (M1, cbdyncluster 7.1-stable, openjdk 11)') {
            agent { label 'm1' }
            environment {
                // Advice from builds team: '"java" doesn't support Linux aarch64. Only openjdk.'
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11_M1}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11_M1}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                test(OPENJDK, OPENJDK_11_M1, "7.1-stable")
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }


        stage('testing (Graviton2, mocks, openjdk 11)') {
            agent { label 'qe-grav2-amzn2' }
            environment {
                // Advice from builds team: '"java" doesn't support Linux aarch64. Only openjdk.'
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11}/bin:${WORKSPACE}/deps/maven-3.5.2-cb6/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_11)
                    // qe-grav2-amzn2 doesn't have maven
                    shWithEcho("cbdep install -d deps maven 3.5.2-cb6")
                    dir('couchbase-jvm-clients') {
                        checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])
                        // Advice from builds team: cbdyncluster cannot be contacted from qe-grav2-amzn2, so testing
                        // against mocks only for now
                        script { testAgainstMock() }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('testing (Alpine, mock, openjdk 11)') {
            agent { label 'alpine' }
            environment {
                JAVA_HOME = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11_M1}"
                PATH = "${WORKSPACE}/deps/${OPENJDK}-${OPENJDK_11_M1}/bin:$PATH"
            }
            when {
                expression
                        { return IS_GERRIT_TRIGGER.toBoolean() == false }
            }
            steps {
                 catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    installJDKIfNeeded(platform, OPENJDK, OPENJDK_11_M1)
                    dir('couchbase-jvm-clients') {
                        checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])
                        // Mock testing only, with native IO disabled - check JVMCBC-942 for details
                        script { testAgainstMock(true) }
                    }
                 }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }


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
            agent { label 'sdkqe-centos7' }
            steps {
                cleanupWorkspace()
                dir('couchbase-jvm-clients') {
                    checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])
                    shWithEcho("mvn clean install -Dmaven.test.skip --batch-mode")
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


void test(Map args=[:],
            String jdk,
            String jdkVersion,
            String serverVersion) {

    boolean includeAnalytics = args.containsKey("includeAnalytics") ? args.get("includeAnalytics") : true
    boolean includeEventing = args.containsKey("includeEventing") ? args.get("includeEventing") : false
    boolean enableDevelopPreview = args.containsKey("enableDevelopPreview") ? args.get("enableDevelopPreview") : false
    boolean ceMode = args.containsKey("ceMode") ? args.get("ceMode") : false
    boolean multiCerts = args.containsKey("multiCerts") ? args.get("multiCerts") : false

    catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
        cleanupWorkspace()
        installJDKIfNeeded(platform, jdk, jdkVersion)

        dir('couchbase-jvm-clients') {
            checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])
            script { testAgainstServer(serverVersion, QUICK_TEST_MODE, includeAnalytics, includeEventing, enableDevelopPreview, ceMode, multiCerts) }
            shWithEcho("make deps-only")
            shWithEcho("mvn clean install -Dmaven.test.skip --batch-mode")
        }
    }
}


void buildScala(String jdk,
                String jdkVersion,
                String scalaCompatVersion,
                String scalaLibraryVersion) {
    catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
        cleanupWorkspace()
        installJDKIfNeeded(platform, jdk, jdkVersion)

        dir('couchbase-jvm-clients') {
            checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])
            shWithEcho("make deps-only")
            shWithEcho("mvn -Dmaven.test.skip --batch-mode -Dscala.compat.version=${scalaCompatVersion} -Dscala.compat.library.version=${scalaLibraryVersion} clean compile")
        }
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
void testAgainstServer(String serverVersion,
                       boolean QUICK_TEST_MODE,
                       boolean includeAnalytics = true,
                       boolean includeEventing = false,
                       boolean enableDevelopPreview = false,
                       boolean ceMode = false,
                       boolean multiCerts = false) {
    def clusterId = null
    try {
        // For debugging
        shIgnoreFailure("echo $JAVA_HOME")
        shIgnoreFailure("ls $JAVA_HOME")
        shIgnoreFailure("echo $PATH")
        shIgnoreFailure("java -version")

        // For debugging, what clusters are open
        shWithEcho("cbdyncluster ps -a")

        // May need to remove some manually if they're stuck.  -f forces, allows deleting cluster we didn't open
        // shWithEcho("cbdyncluster rm -f 3d023261")

        // Allocate the cluster
        def script = "cbdyncluster allocate --num-nodes=3 ${ceMode ? ' --use-ce=true' : ''} --server-version=${serverVersion}"
        echo "Running " + script
        clusterId = sh(script: script, returnStdout: true).trim()
        echo "Got cluster ID $clusterId"

        // Find the cluster IP
        def ips = sh(script: "cbdyncluster ips $clusterId", returnStdout: true).trim()
        echo "Got raw cluster IPs " + ips
        def ip = ips.tokenize(',')[0]
        echo "Got cluster IP http://" + ip + ":8091"

        // By default Java and Scala use mock for testing, make them use cbdyncluster instead
        createIntegrationTestPropertiesFile('java-client/src/integrationTest/resources/integration.properties', ip)
        createIntegrationTestPropertiesFile('scala-client/src/integrationTest/resources/integration.properties', ip)

        // We need a bit more than the default 600 for out bucket management integration tests
        // Particularly Magma which requires min of 1GB
        def ramQuota = 1500

        // Create the cluster
        if (!QUICK_TEST_MODE) {
            def services = "kv,index,n1ql,fts${includeAnalytics ? ',cbas' : ''}${includeEventing ? ',eventing' : ''}"
            shWithEcho("cbdyncluster --node $services --node kv --node kv --bucket default --ram-quota $ramQuota setup $clusterId")
        } else {
            // During development, this is faster (less tests)
            shWithEcho("cbdyncluster --node kv --node kv --node kv --bucket default --ram-quota $ramQuota setup $clusterId")
        }

        // Make sure the cluster stays up during all tests (the finally block below ensures that it's always pulled down)
        shWithEcho("cbdyncluster refresh $clusterId 2h")

        // Just for debugging, log some cluster details
        try {
            shWithEcho("curl -u Administrator:password http://" + ip + ":8091/pools")
            shWithEcho("curl -u Administrator:password http://" + ip + ":8091/pools/default")
        }
        catch (RuntimeException ex) {
            echo "Exception while getting debugging info ${ex}"
        }

        // Make the bucket flushable
        shWithEcho("curl -v -X POST -u Administrator:password -d flushEnabled=1 http://" + ip + ":8091/pools/default/buckets/default")

        // Set the query indexer mode.  Without this query tests fail with "GSI CreatePrimaryIndex() - cause: Please Set Indexer Storage Mode Before Create Index"
        shWithEcho("curl -v -X POST -u Administrator:password -d 'storageMode=${ceMode ? 'forestdb' : 'plasma'}' http://" + ip + ":8091/settings/indexes")

        if (enableDevelopPreview) {
            shWithEcho("curl -v -X POST -u Administrator:password -d 'enabled=true' http://" + ip + ":8091/settings/developerPreview")
        }

        if (multiCerts) {
            shWithEcho("mkdir certs")
            shWithEcho("cbdyncluster setup-cert-auth $clusterId --user Administrator --num-roots 2 --out-dir=certs")
            shWithEcho("echo 'cluster.unmanaged.certsFile=${WORKSPACE}/couchbase-jvm-clients/certs/ca.pem' >> java-client/src/integrationTest/resources/integration.properties")
            shWithEcho("echo 'cluster.unmanaged.certsFile=${WORKSPACE}/couchbase-jvm-clients/certs/ca.pem' >> scala-client/src/integrationTest/resources/integration.properties")
        }

        // Not sure why this is needed, it should be in stash from build....
        shWithEcho("make deps-only")

        // The --batch-mode hides download progress messages, very verbose
        if (!QUICK_TEST_MODE) {
            shWithEcho("mvn --fail-at-end clean install --batch-mode -Dgroups=!flaky")
        } else {
            // This is for iteration during development, skips out some steps
            shWithEcho("mvn -pl '!scala-client,!scala-implicits' --fail-at-end clean install test --batch-mode")

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

void testAgainstMock(boolean disableNativeIo = false) {
    shWithEcho("make deps-only")
    shWithEcho("mvn --fail-at-end clean install --batch-mode ${disableNativeIo ? '-Dcom.couchbase.client.core.deps.io.netty.transport.noNative=true' : ''}")
}

void cleanupWorkspace() {
    dir("${workspace}") {
        sh 'ls'
    }

    // This _might_ clean the workspace now, or it may clean it after the build: the docs are unclear: one place says
    // "Delete workspace when build is done", another indicates it's an imperative command.
    // So, also doing the deleteDir() steps below, perhaps redundantly.
    cleanWs()

    dir("${workspace}") {
        sh 'ls'
        deleteDir()
        sh 'ls'
    }

    // Per https://stackoverflow.com/questions/37468455/jenkins-pipeline-wipe-out-workspace, there are directories the
    // above will not delete
    dir("${workspace}@tmp") {
        deleteDir()
    }

    dir("${workspace}@script") {
        deleteDir()
    }
}