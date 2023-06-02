pipeline {
    agent none
    stages {
        stage('Test') {
            agent {
                dockerfile {
                    args '-u root:root'
                }
            }
            steps {
                sh "sbt compile"
                sh "sbt test"
            }
        }
    }
    post {
        always {
            script {
                node {
                    junit allowEmptyResults: true, testResults: 'target/test-reports/*.xml'
                }
            }
        }
    }
}

