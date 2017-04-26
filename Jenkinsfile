pipeline {
  agent any

  stages {

    stage('Build') {
      steps {
        echo 'Checking out code...'
        checkout scm
        echo 'make clean'
        sh 'make clean'
      }
    }

    stage('Test') {
      steps {
        echo 'Run flake tests...'
        sh 'make flake'
      }
    }

    stage('Publish') {
      when {
        branch 'master'
      }
      steps {
        echo 'Publishing to pypi'
        sh 'make upload' 
      }
    }
  }
}