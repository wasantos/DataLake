pipeline{
    agent { label 'slave_local' }
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "## Limpando o Workspace ##"'
                deleteDir()
            }
        }

        stage('SCM GitHub - Checkout'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## SCM GitHub - Checkout ##"'
                    git branch: 'master',
                    credentialsId: 'd319fe2f-a4b7-4e8c-8b30-2803211f33c4',
                    url: 'https://github.com/wasantos/DataLake.git'
                }
            }  
        }
        
            stage('Check Python'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## Clean Project ##"'
                    sh 'pwd'
                    sh 'pyenv local 3.6.6'
                    sh 'python --version'
                    
                }
            }
        }  

        stage('Find directory to build'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## Find directory to build ... ##"'
                    sh 'pwd'
                    sh 'tree'
                }
            }
        }
        
        stage('Build DataLake'){
            steps{
                dir('projeto/datalake'){
                    sh 'echo -e "## Build DataLake Python ##"'
                    sh 'pwd'
                    sh 'python build.py'
                    
                 }
            }
        }
                    
        stage('Post Building') {
            steps {
                    sh 'echo -e "## Aqui código do Post-Building ##"'
                    // One or more steps need to be included within the steps block.
                  }

        post {
        success {
                   sh 'echo -e "## Se der tudo Certo!!! ...Teste webhook ....##"'
                    // One or more steps need to be included within each condition's block.
                }
            }
        
        }
    }
}
