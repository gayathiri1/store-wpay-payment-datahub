# Steps/Info
We are using the Github as source control repositry and cloud build for the CICD pipeline. Github repo would be monitored by the cloud build triggers and when the conditions satisfy the build triggers the pipelines. In the Github repo we would be having the below branching stratergies.
1. prod --> This would be our master branch from where the prod deployments would happen.
2. pre-prod --> From this branch the pre-prod deployments would happen.
3. dev --> From this branch the dev deployment would happen.
4. feature/DATPAY-xxxx --> Developers would create a feature branch from dev branch with this naming format(where DATPAT-xxxx is jira ticket number) to build the new feature based on the Jira ticket. From this branch developers need to create the pull request(PR) with the dev branch. At this step the validation pipeline would get triggered(pull-req-cloudbuild.yaml).
    a. Developer need to check the outcome of this pipeline in the cloud build and verify only the dags and etls which are modified in the current feature change are shown in the result.
    b. The person who is going to merge this pull request(Currently only Rupesh would be having this acces) would need to varify the validation pipeline outcome before approving the pipeline to make sure the developers have done the right thing.
    c. Once the merge request is approved the merge trigger(merge-req-cloudbuild.yaml) would get invoked and to the deployment of the dags and etls.
5. release/x.x --> Once the dev testing the complete, <b>devolper need to create the release branch from prod branch </b> and move the changes to this branch which need to be deployed in preprod/prod. This release branch could have changes releated to either one feature or multiple features.    
    a. Again similar to step 4, developer need to create the pull request first with preprod branch for deploying it to preprod and validate the pipeline outcome.
    b. The person who is going to approve the merge request would validate the pipeline results and approve the pull request.
    c. This would trigger the deploy pipeline(merge-req-cloudbuild.yaml) and would <b>deploy the dags and etl to preprod</b>.
    d. Once the preprod testing is complete developer need to create the pull request with the prod branch and validate the pipeline results.
    e. Again the person who is going to approve the merge request would validate the pipeline results and approve the pull request.
    f. This would trigger the deploy pipeline(merge-req-cloudbuild.yaml) and would <b>deploy the dags and etl to prod</b>.

<b>Steps to Connect to GitHub Repo</b>(One time activity):
1. Enable Cloud Build API if not done already.
2. Install Cloud Build App:
	a. Open Triggers in Cloud Build and after selecting the Cloud Repo, Select 'Connect Repository'.
	b. Select 'GitHub (Cloud Build GitHub App)' and Authorize Google Cloud Build App to connect the Google Cloud(For the first time). 
	c. Click Install Cloud Build App.
	d. In the pop-up select GitHub account username or organization and Select either all repositories or particular repository/ies. 
3. Select the GitHub Account and Repository, click Connect.

<b>Steps to Connect to Clone complete Repo</b>:
The cloud build only copies the current commit to the cloudbuild workspace, it doesn't copy the history. So for git clone in cloud build we need to use the ssh key to connect to git hub. Followed below steps from url: https://cloud.google.com/build/docs/access-github-from-build
1. Generate ssh-keygen -t rsa -b 4096 -N '' -f id_github -C <email_id>
2. Copy the private key to Secret Manager. Create the secret with the name as $REPO_NAME-github-key
3. Copy the public to Git Repository --> Repo --> Setttings --> Deploy Keys
4. Give permission to cloudbuild service account with secret manager accessors role. => Handled in terraform
5. Create known_host file using: ssh-keyscan -t rsa github.com > known_hosts.github => Handled in cloud build yaml file.

<b>Steps to Create Trigger</b>: Terraform is used to create trigger as in each(dev, preprod and prod) environment two triggers need to be created, 

    a. one for pull request(this will be triggered when developer creates the pull request, to present the validation outcome to the person who is going to approve the pull request).
    b. another one for merge/push request(this will be triggered when the pull request is approved i.e, when the merge happens).

1. Navigate to the path <i>'infra/terraform'</i> of this repository, either from cloudshell or from local(need to have access to gcp to run terraform).
2. Run terraform init command. <i>terraform init -backend-config=backend/dev.tf</i>
    Note: Replace dev.tf with preprod.tf for preprod and prod.tf for prod.
3. Run terraform plan command. <i>terraform plan --var-file=vars/dev.tfvars</i>
    Note: Replace dev.tfvars with preprod.tfvars for preprod and prod.tfvars for prod.
4. Run terraform apply command to create the triggers. Replace <i>plan</i> with <i>apply</i> in the above command.


<b>git hooks</b>:
In github we don't have option to restrict the new branch creation if they don't match our naming standards. Hence we need to create the git hooks to validate our branch naming standards but the hooks are stored in '.git/hooks' folder and they are not version controlled, meaning the .git folder is not pushed to server/github. Hence every developer need to create push git hook file in thier local to get it executed by default. Instead of this we moved the scripts to .githooks folder and pushed this folder to github. Even this appraoch user need to change their default hook directory by running this command.
git config core.hooksPath .githooks

Note: Additionally we are doing this branch name validation in the pipeline as well.

<b>Sample bq load command</b>: 

    bq load --source_format=CSV  --field_delimiter=$(printf ';') sample_ds.two  two.csv

<b>Few URLs </b>: 
1. https://cloud.google.com/blog/topics/developers-practitioners/using-cloud-build-keep-airflow-operators-date-your-composer-environment --> Fir updating the python packages of composer.
2. https://cloud.google.com/architecture/cicd-pipeline-for-data-processing --> CI-CD pipeline example for data processing.
3. https://engineering.ripple.com/building-ci-cd-with-airflow-gitlab-and-terraform-in-gcp/ --> CI-CD with gitlab.
4. https://alexandre-slp.medium.com/composer-ci-cd-pipeline-with-cloud-build-and-python-script-9ab888138e69 --> Composer CI-CD with cloud build and python script.
5. https://www.springml.com/blog/developing-continuous-integration-for-google-cloud-composer/ --> Composer CI-CD with Jenkins.
6. https://github.com/jaketf/ci-cd-for-data-processing-workflow --> CI-CD sample
7. https://www.youtube.com/watch?v=3vfXQxWJazM --> Terraform validation options and templates.


<b>Notes/Review Comments</b>:
1. If tables already exists in the bq, just exit with failure.
2. Logs need to go to cloud logs, not to the bucket.
3. Document the process.

