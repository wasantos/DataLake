pipeline{
    agent { label 'slave-local' }
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "\033[0;34m## Limpando o Workspace ##\033[0m"'
                deleteDir()
            }
        }

        stage('SCM - GitHub'){
            steps{
                dir('projeto'){
                    sh 'echo -e "\033[0;34m ## Innersource Checkout ##\033[0m"'
                    git branch: 'master',
                    credentialsId: '9a54ae94-57c6-46ae-9ce0-4974a758182d',
                    url: 'https://github.com/wasantos/teste.git'
                }
            }  
        }

        stage('Build Datalake'){
            steps{
                dir('projeto'){
                    sh 'echo -e "\033[0;34m ## Build Datalake ##\033[0m"'
                    sh 'pyenv local 3.6.6'
                    sh 'cd datalake'
                    sh 'python --version'
                    sh 'python build.py'
                }
            }
        }
    }
}