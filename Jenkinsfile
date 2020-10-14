@Library('JenkinsMain@2.16.40')_

pipelinePythonSCA(
    agentLabel: 'pylint',
    pythonVersion: '3.6',
    packages: ['.'],
    runUnitTests: false,
    runBlack: true,
    runIsort: false,
    installFromSetup: true,
)
