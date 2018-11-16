pipeline{
    agent { label 'master' }
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "## Limpando o Workspace ##"'
                deleteDir()
            }
        }

        stage('SCM - GitHub'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## Innersource Checkout ##"'
                    git branch: 'master',
                    credentialsId: '9a54ae94-57c6-46ae-9ce0-4974a758182d',
                    url: 'https://github.com/wasantos/DataLake.git'
                }
            }  
        }

        stage('Build Datalake'){
            steps{
                dir('datalake'){
                    sh 'echo -e "## Build Datalake ##"'
                    sh 'pwd'
                    sh 'python --version'
                    sh 'python build.py'
                    sh 'echo "Fim ..."'
                }
            }
        }
    }
}