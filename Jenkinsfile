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
            steps {
                buildScala(defaultBuildJvm(), "2.13", REFSPEC)
            }
        }

        stage('Build Scala 3 (OpenJDK 17)') {
            agent { label "sdkqe" }
            steps {
                buildScala(defaultBuildJvm(), "3", REFSPEC)
            }
        }

        stage('Platform testing (Graviton2, mocks, openjdk 11)') {
            agent { label 'qe-grav2-amzn2' }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
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
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
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
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
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

        stage('Platform testing (ARM Ubuntu 20, mock, openjdk 17)') {
            agent { label 'qe-ubuntu20-arm64' }
            steps {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    cleanupWorkspace()
                    dir('couchbase-jvm-clients') {
                        doCheckout(REFSPEC)
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


        stage('Platform testing (ARM Ubuntu 22, mock, openjdk 17)') {
                    agent { label 'qe-ubuntu22-arm64' }
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            cleanupWorkspace()
                            dir('couchbase-jvm-clients') {
                                doCheckout(REFSPEC)
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


        stage('Platform testing (ARM RHEL 9, mock, openjdk 17)') {
                    agent { label 'qe-rhel9-arm64' }
                    steps {
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            cleanupWorkspace()
                            dir('couchbase-jvm-clients') {
                                doCheckout(REFSPEC)
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
    }
    post {
        failure { emailFailure() }
        success { emailSuccess() }
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
