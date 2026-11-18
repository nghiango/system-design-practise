// Rendered by ci/scripts/register_jenkins_jobs.py (__TOPICS__ and __SOURCE_URL__ are replaced).
// Idempotent: creates or refreshes folder system-design/, one job per topic, and topic-switch.
import com.cloudbees.hudson.plugins.folder.Folder
import groovy.json.JsonOutput
import hudson.model.ChoiceParameterDefinition
import hudson.model.ParametersDefinitionProperty
import hudson.plugins.git.BranchSpec
import hudson.plugins.git.GitSCM
import hudson.plugins.git.UserRemoteConfig
import hudson.plugins.git.extensions.impl.LocalBranch
import hudson.plugins.git.extensions.impl.PathRestriction
import hudson.triggers.SCMTrigger
import jenkins.model.Jenkins
import org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition
import org.jenkinsci.plugins.workflow.job.WorkflowJob

def topics = __TOPICS__
def sourceUrl = '__SOURCE_URL__'
def controller = Jenkins.get()
assert controller.isUseSecurity(): 'Refusing to configure an unsecured controller'

def folder = controller.getItem('system-design')
if (folder == null) {
    folder = controller.createProject(Folder.class, 'system-design')
}
assert folder instanceof Folder: 'Existing item system-design is not a folder'
folder.setDescription('System design practise topics: one build/deploy job per topic, plus topic-switch to choose the single online topic.')
folder.save()

def scmFor = { List extensions ->
    new GitSCM([new UserRemoteConfig(sourceUrl, null, null, null)], [new BranchSpec('*/main')],
               false, [], null, null, [new LocalBranch('main')] + extensions)
}
def pipelineJob = { String name ->
    def job = folder.getItem(name) ?: folder.createProject(WorkflowJob.class, name)
    assert job instanceof WorkflowJob: "Existing item system-design/${name} is not a Pipeline job"
    job
}
// A topic job builds when its own folder or anything shared by every topic changes.
def shared = ['build-logic/.*', 'gradle/.*', 'settings\\.gradle\\.kts', 'build\\.gradle\\.kts', 'ci/.*', 'deploy/.*']

def created = []
topics.each { topic ->
    def job = pipelineJob(topic)
    def regions = (["topics/${topic}/.*"] + shared).join('\n')
    def definition = new CpsScmFlowDefinition(scmFor([new PathRestriction(regions, '')]), 'ci/Jenkinsfile')
    definition.setLightweight(false)
    job.setDefinition(definition)
    job.setDescription("Build, test and publish topic ${topic} to GitOps. Going online is done with system-design/topic-switch.")
    job.setTriggers([new SCMTrigger('H/2 * * * *')])
    job.save()
    created << job.fullName
}

def switchJob = pipelineJob('topic-switch')
def switchDefinition = new CpsScmFlowDefinition(scmFor([]), 'ci/Jenkinsfile.switch')
switchDefinition.setLightweight(true)
switchJob.setDefinition(switchDefinition)
switchJob.setDescription('Choose which single topic is online (writes topics/active.yaml in the GitOps repo).')
switchJob.setTriggers([])
switchJob.removeProperty(ParametersDefinitionProperty.class)
switchJob.addProperty(new ParametersDefinitionProperty([
    new ChoiceParameterDefinition('ACTION', ['status', 'activate', 'deactivate'] as String[],
        'status: show what is online; activate: put TOPIC online (switches the current one off); deactivate: nothing online'),
    new ChoiceParameterDefinition('TOPIC', topics as String[], 'Topic to activate (ignored by status/deactivate)')
]))
switchJob.save()
created << switchJob.fullName

println JsonOutput.toJson([jobs: created, source: sourceUrl])
