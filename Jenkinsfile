// Want to make this matrix/parameterised, but:
// - Cannot make a proper matrix Jenkins project that's also pipeline
// - Seems to be no way to do e.g. multiple JDKs in a declarative pipeline
// - Can do it with scripted pipeline, but anything inside a node {} block won't trigger the post block, so can't gather junit results
// So, for now, everything is hard-coded.  It's unlikely to change often.
// TODO: stashing the junit file after its generated and unstashing it in post, may work
LINUX_AGENTS = 'centos6||centos7||ubuntu16||ubuntu14||ubuntu20'
QUICK_TEST_MODE = false // enable to support quicker development iteration

// Java versions available through cbdeps are on
// https://hub.internal.couchbase.com/confluence/pages/viewpage.action?spaceKey=CR&title=cbdep+available+packages
// https://github.com/couchbasebuild/cbdep/blob/master/cbdep.config

Jvm oracle8() { oracle('8u192') } // Avoid above Oracle 8u201, for licensing reasons.
Jvm oracle11() { oracle('11.0.3') }

Jvm openjdk8() { openjdk('8u292-b10') } // https://github.com/AdoptOpenJDK/openjdk8-binaries/releases
Jvm openjdk11() { openjdk('11.0.23+9') }
Jvm openjdk17() { openjdk('17.0.11+9') }
Jvm openjdk21() { openjdk('21.0.3+9') }

Jvm corretto8() { corretto('8.232.09.1') } // available versions: https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/doc-history.html
Jvm corretto11() { corretto('11.0.5.10.1') } // available versions: https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/doc-history.html

Jvm oracle(String version) { new Jvm('java', version) }
Jvm openjdk(String version) { new Jvm('openjdk', version) }
Jvm corretto(String version) { new Jvm('corretto', version) }

Jvm defaultBuildJvm() { openjdk17() }

class Jvm implements Serializable {
    final String cbdepPackage
    final String version

    Jvm(String cbdepPackage, String version) {
        this.cbdepPackage = cbdepPackage
        this.version = version
    }

    @NonCPS String toString() { cbdepPackage + ' ' + version }
}

// Ideally these would be instance methods of Jvm,
// but the Jenkins CPS execution model makes it a hassle.
String javaHome(Jvm jvm) { "$WORKSPACE/deps/${jvm.cbdepPackage}-${jvm.version}" }
String javaBinary(Jvm jvm) { javaHome(jvm) + '/bin/java' }
String pathSeparator() { isUnix() ? ':' : ';' }
String pathFor(Jvm jvm) { javaHome(jvm) + '/bin' + pathSeparator() + PATH }

void shOrBat(String command) { isUnix() ? sh(command) : bat(command) }

void installJvm(Jvm jvm) {
    dir(WORKSPACE) {
        if (fileExists(javaHome(jvm))) {
            echo "$jvm already installed"
        } else {
            echo "installing $jvm"
            shOrBat('cbdep --version')
            shOrBat('cbdep --debug platform')
            shOrBat("cbdep --debug install -d deps ${jvm.cbdepPackage} ${jvm.version}")
        }
        shOrBat(javaBinary(jvm) + ' -version') // smoke test
    }
}

String setEnv(String name, String value) { isUnix() ? "${name}=\"${value}\"" : "set ${name}=${value}\r\n" }

String setEnvFor(Jvm jvm) {
    // Unix wants all on same line. Windows wants separate lines.
    // Let `setEnv()` insert the line ending for Windows.
    setEnv('JAVA_HOME', javaHome(jvm)) + ' ' + setEnv('PATH', pathFor(jvm))
}

void makeDeps(Jvm buildJvm) {
    installJvm(buildJvm)
    shOrBat("${setEnvFor(buildJvm)} make deps-only")
}

void runMaven(Jvm buildJvm, Jvm testJvm, String mavenArgs) {
    installJvm(buildJvm)
    if (testJvm != null) installJvm(testJvm)

    String command = setEnvFor(buildJvm) + ' ./mvnw '
    shOrBat(command + '--version')

    command += "--batch-mode " // Hide verbose download progress messages.

    // Specify the Java executable to use when running tests.
    // See https://maven.apache.org/surefire/maven-surefire-plugin/test-mojo.html (Search for "<jvm>").
    if (testJvm != null) command += "-Djvm=${javaBinary(testJvm)} "

    command += mavenArgs
    shOrBat(command)
}

boolean triggeredByGerrit() { IS_GERRIT_TRIGGER.toBoolean() }
boolean notTriggeredByGerrit() { !triggeredByGerrit() }

// The latest released cluster version.  The majority of the testing is done against this.
def CLUSTER_VERSION_LATEST_STABLE = "7.1-stable"

// The lucky spammees
EMAILS = ['graham.pople@couchbase.com', 'david.nault@couchbase.com']

pipeline {
    agent none

    options {
        // Safety check, prevent the script running forever
        timeout(time: 600, unit: 'MINUTES')
    }

    stages {
        stage('Build Scala 2.13 (OpenJDK 17)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                buildScala(defaultBuildJvm(), "2.13", REFSPEC)
            }
        }

        stage('Build Scala 3 (OpenJDK 17)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                buildScala(defaultBuildJvm(), "3", REFSPEC)
            }
        }

        // Test against mock - this skips a lot of tests, and is intended for quick validation
        stage('Validation testing (mock, Oracle JDK 8)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { triggeredByGerrit() }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()

                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
                        // By default Java and Scala use mock for testing
                        runMaven(defaultBuildJvm(), oracle8(), "-Dmaven.test.failure.ignore=true clean test")

                        // While iterating Jenkins development, this makes it much faster:
                        // runMaven(defaultBuildJvm(), oracle8(), "package surefire:test -Dtest=com.couchbase.client.java.ObserveIntegrationTest -pl java-client")
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

        // "JDK testing"         = checking the latest stable cluster against various JDK versions
        // "Cluster testing"     = checking a specific cluster version
        // "JDK/Cluster testing" = orthogonally testing JDKs and clusters was expensive in time, so they are now combined.
        //                         An arbitrary JDK is used for each cluster - aim is just to get good coverage of all of them.
        // "Platform testing"    = checking a specific platform (M1, ARM, Alpine etc.)
        // "CE testing"          = Community Edition testing


        // Test against cbdyncluster - do for nightly tests
        // One day can get all these cbdyncluster tests running in parallel: https://jenkins.io/blog/2017/09/25/declarative-1/

        // No cluster testing for CLUSTER_VERSION_LATEST_STABLE since that is thoroughly tested by JVM tests
        // No cluster testing for non-serverless 7.5, as that is dedicated to serverless

        stage('JDK/Cluster testing  (Linux, cbdyncluster 7.6-stable, OpenJDK 21)') {
             agent { label "sdkqe" }
             when {
                 beforeAgent true
                 expression { notTriggeredByGerrit() }
             }
             steps {
                 test(defaultBuildJvm(), openjdk21(), "7.6-stable", REFSPEC)
             }
             post {
                 always {
                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                 }
             }
         }

        stage('Cluster testing  (Linux, cbdyncluster 7.6-stable, Oracle JDK 8)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), oracle8(), "7.6-stable", REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('JDK/Cluster testing  (Linux, cbdyncluster 7.2-stable, Oracle JDK 8)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), oracle8(), "7.2-stable", REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('JDK/Cluster testing  (Linux, cbdyncluster 7.1-stable, openjdk 17)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), openjdk17(), "7.1-stable", REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('JDK/Cluster testing  (Linux, cbdyncluster 7.0-stable, Oracle JDK 11)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), oracle11(), "7.0-stable", REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('JDK/Cluster testing  (Linux, cbdyncluster 6.6-stable, Corretto 8)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), corretto8(), "6.6-stable", REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        // 6.5 is EOL, we do one sanity test against it
        stage('JDK/Cluster testing  (Linux, cbdyncluster 6.5-release, Corretto 11)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), corretto11(), "6.5-release", REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        // When removing tests for an older cluster version, if it's a JDK/Cluster test please
        // make sure that JDK is still tested.

        // 7.5 is dedicated to serverless
        // Cannot be run due to issues with indexer service in --serverless-mode: https://couchbase.slack.com/archives/CFM4D3VFU/p1689590660024819
        // Real Elixir is tested in job: http://qe-jenkins.sc.couchbase.com/job/DirectNebulaJob-centos-sdk/
//         stage('Serverless testing (Linux, cbdyncluster 7.5-stable Serverless mode, Oracle JDK 8)') {
//             agent { label "sdkqe" }
//             when {
//                 beforeAgent true
//                 expression { notTriggeredByGerrit() }
//             }
//             steps {
//                 test(defaultBuildJvm(), oracle8(), "7.5-stable", includeEventing : true, serverlessMode: true, REFSPEC)
//             }
//             post {
//                 always {
//                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                 }
//             }
//         }

        // Cannot use 7.1-stable, it maps to 7.1.3 and there is no 7.1.3 CE release.  7.1.1 is current latest (Nov '22).
        stage("CE testing (Linux, cbdyncluster 7.1.1, OpenJDK 8)") {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), openjdk8(), "7.1.1", ceMode : true, REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        // 7.0.3 does not and will not have a CE build.
        stage('CE testing (Linux, cbdyncluster 7.0.2, Oracle JDK 8)') {
            agent { label "sdkqe" }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                test(defaultBuildJvm(), oracle8(), "7.0.2", ceMode : true, REFSPEC)
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }


// The Maven wrapper can't install itself here due to a broken `wget` installation
//        stage("Platform testing (M1, stable, openjdk 17)") {
//            agent { label 'm1' }
//            when {
//                beforeAgent true
//                expression { notTriggeredByGerrit() }
//            }
//            steps {
//                test(defaultBuildJvm(), openjdk17(), CLUSTER_VERSION_LATEST_STABLE, REFSPEC)
//            }
//            post {
//                always {
//                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                }
//            }
//        }

        stage('Platform testing (Graviton2, mocks, openjdk 11)') {
            agent { label 'qe-grav2-amzn2' }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
                        // Advice from builds team: cbdyncluster cannot be contacted from qe-grav2-amzn2, so testing
                        // against mocks only for now
                        script { testAgainstMock(defaultBuildJvm(), openjdk11()) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('Platform testing (Graviton3, mocks, openjdk 17)') {
            agent { label 'qe-grav3-amzn2' }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
                        // Advice from builds team: cbdyncluster cannot be contacted from graviton agent, so testing
                        // against mocks only for now
                        script { testAgainstMock(defaultBuildJvm(), openjdk17()) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

        stage('Platform testing (Graviton4, mocks, openjdk 17)') {
            agent { label 'qe-grav4-amzn2' }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
                        // Advice from builds team: cbdyncluster cannot be contacted from graviton agent, so testing
                        // against mocks only for now
                        script { testAgainstMock(defaultBuildJvm(), openjdk17()) }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }

// Temporarily disabling until JVMCBC-1227 is resolved
//         stage('Platform testing (Alpine, mock, openjdk 11)') {
//             agent { label 'alpine' }
//             when {
//                 beforeAgent true
//                 expression { notTriggeredByGerrit() }
//             }
//             steps {
//                  catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
//                     cleanupWorkspace()
//                     dir('couchbase-jvm-clients') {
//                         doCheckout(REFSPEC)
//                         // Mock testing only, with native IO disabled - check JVMCBC-942 for details
//                         script { testAgainstMock(defaultBuildJvm(), openjdk11(), true) }
//                     }
//                  }
//             }
//             post {
//                 always {
//                     junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
//                 }
//             }
//         }

        stage('Platform testing (ARM Ubuntu 20, mock, openjdk 17)') {
            agent { label 'qe-ubuntu20-arm64' }
            when {
                beforeAgent true
                expression { notTriggeredByGerrit() }
            }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
                        // Cbdyn not available on this machine
                        script {
                            makeDeps(defaultBuildJvm())
                            runMaven(defaultBuildJvm(), openjdk17(), "--fail-at-end clean install")
                        }
                    }
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                }
            }
        }


        stage('Platform testing (ARM Ubuntu 22, mock, openjdk 17)') {
                    agent { label 'qe-ubuntu22-arm64' }
                    when {
                        beforeAgent true
                        expression { notTriggeredByGerrit() }
                    }
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            cleanupWorkspace()
                            dir('couchbase-jvm-clients') {
                                doCheckout(REFSPEC)
                                // Cbdyn not available on this machine
                                script {
                                    makeDeps(defaultBuildJvm())
                                    runMaven(defaultBuildJvm(), openjdk17(), "--fail-at-end clean install")
                                }
                            }
                        }
                    }
                    post {
                        always {
                            junit allowEmptyResults: true, testResults: '**/surefire-reports/*.xml'
                        }
                    }
                }


        stage('Platform testing (ARM RHEL 9, mock, openjdk 17)') {
                    agent { label 'qe-rhel9-arm64' }
                    when {
                        beforeAgent true
                        expression { notTriggeredByGerrit() }
                    }
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            cleanupWorkspace()
                            dir('couchbase-jvm-clients') {
                                doCheckout(REFSPEC)
                                // Cbdyn not available on this machine
                                script {
                                    makeDeps(defaultBuildJvm())
                                    runMaven(defaultBuildJvm(), openjdk17(), "--fail-at-end clean install")
                                }
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
//             when {
//                 beforeAgent true
//                 expression { notTriggeredByGerrit() }
//             }
//             steps {
//                 catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
//                     cleanWs()
//                     unstash 'couchbase-jvm-clients'
//
//                     dir('couchbase-jvm-clients') {
//                         script {
//                             testAgainstServer(defaultBuildJvm(), oracle8(), SERVER_TEST_VERSION, QUICK_TEST_MODE)
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
    }
    post {
        failure { emailFailure() }
        success { emailSuccess() }
    }
}


void test(Map args=[:],
            Jvm buildJvm,
            Jvm testJvm,
            String serverVersion,
            String refspec) {

    boolean ceMode = args.containsKey("ceMode") ? args.get("ceMode") : false
    boolean includeAnalytics = args.containsKey("includeAnalytics") ? args.get("includeAnalytics") : !ceMode // CE doesn't have analytics
    boolean includeEventing = args.containsKey("includeEventing") ? args.get("includeEventing") : false
    boolean enableDevelopPreview = args.containsKey("enableDevelopPreview") ? args.get("enableDevelopPreview") : false
    boolean multiCerts = args.containsKey("multiCerts") ? args.get("multiCerts") : false
    boolean serverlessMode = args.containsKey("serverlessMode") ? args.get("serverlessMode") : false

    catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
        cleanupWorkspace()

        dir('couchbase-jvm-clients') {
            doCheckout(refspec)
            script { testAgainstServer(buildJvm, testJvm, serverVersion, QUICK_TEST_MODE, includeAnalytics, includeEventing, enableDevelopPreview, ceMode, multiCerts, serverlessMode) }
        }
    }
}


void buildScala(Jvm buildJvm,
                String scalaCompatVersion,
                String refspec) {
    catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
        cleanupWorkspace()

        dir('couchbase-jvm-clients') {
            doCheckout(refspec)
            makeDeps(buildJvm)
            runMaven(buildJvm, null, "-Dmaven.test.skip -Pscala-${scalaCompatVersion} clean compile")
        }
    }
}

void doCheckout(refspec) {
    checkout([$class: 'GitSCM', userRemoteConfigs: [[url: '$REPO']]])

    if (refspec != '' && refspec != null) {
        echo 'Applying REFSPEC'
        checkout([$class: 'GitSCM', branches: [[name: "FETCH_HEAD"]], userRemoteConfigs: [[refspec: "$refspec", url: "https://review.couchbase.org/couchbase-jvm-clients"]]])
        sh(script: "git log -n 2")
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
    if (!isUnix()) {
        echo bat(script: command, returnStdout: true)
    } else {
        echo sh(script: command, returnStdout: true)
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
void testAgainstServer(Jvm buildJvm,
                       Jvm testJvm,
                       String serverVersion,
                       boolean QUICK_TEST_MODE,
                       boolean includeAnalytics = true,
                       boolean includeEventing = false,
                       boolean enableDevelopPreview = false,
                       boolean ceMode = false,
                       boolean multiCerts = false,
                       boolean serverlessMode = false) {
    def clusterId = null
    try {
        // For debugging, what clusters are open
        shWithEcho("cbdyncluster ps -a")

        // May need to remove some manually if they're stuck.  -f forces, allows deleting cluster we didn't open
        // shWithEcho("cbdyncluster rm -f 3d023261")

        // Allocate the cluster
        def script = "cbdyncluster allocate --num-nodes=3 ${ceMode ? ' --use-ce=true' : ''} --server-version=${serverVersion}"
        if (serverlessMode) {
            script += " --serverless-mode"
       }
        echo "Running " + script
        clusterId = sh(script: script, returnStdout: true).trim()
        echo "Got cluster ID $clusterId"

        // Find the cluster IP
        def ips = sh(script: "cbdyncluster ips $clusterId", returnStdout: true).trim()
        echo "Got raw cluster IPs " + ips
        def ip = ips.tokenize(',')[0]
        echo "Got cluster IP http://" + ip + ":8091"

        // By default Java and Scala use mock for testing, make them use cbdyncluster instead
        createIntegrationTestPropertiesFile('core-io/src/integrationTest/resources/integration.properties', ip)
        createIntegrationTestPropertiesFile('java-client/src/integrationTest/resources/integration.properties', ip)
        createIntegrationTestPropertiesFile('scala-client/src/integrationTest/resources/integration.properties', ip)
        createIntegrationTestPropertiesFile('kotlin-client/src/integrationTest/resources/integration.properties', ip)

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
        makeDeps(buildJvm)

        if (!QUICK_TEST_MODE) {
            runMaven(buildJvm, testJvm, "-Dmaven.test.failure.ignore=true clean install -Dgroups=!flaky")
        } else {
            // This is for iteration during development, skips out some steps
            runMaven(buildJvm, testJvm, "-pl '!scala-client,!scala-implicits' --fail-at-end clean install test")

            // Another iteration option, this runs just one test
            //runMaven(buildJvm, testJvm, "package surefire:test -Dtest=com.couchbase.client.java.ObserveIntegrationTest -pl java-client")
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

void testAgainstMock(Jvm buildJvm, Jvm testJvm, boolean disableNativeIo = false) {
    makeDeps(buildJvm)
    runMaven(buildJvm, testJvm, "-Dmaven.test.failure.ignore=true clean install ${disableNativeIo ? '-Dcom.couchbase.client.core.deps.io.netty.transport.noNative=true' : ''}")
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
