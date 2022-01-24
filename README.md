# Steps/Info
We are using the Github as source control repositry and cloud build for the CICD pipeline. Github repo would be monitored by the cloud build triggers and when the conditions satisfy the build triggers the pipelines gets started. In the Github repo we would be having the below branching stratergies.
1. prod --> This would be our master branch from where the prod deployments would happen.
2. pre-prod --> From this branch the pre-prod deployments would happen.
3. dev --> 

<b>Steps to Connect to GitHub Repo</b>:
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

<b>Steps to Create Trigger</b>: Terraform is used to create trigger as in each(pre-prod and prod) environment two triggers need to be created, 

    a. one for pull request(this will be triggered when developer creates the pull request, to present the validation outcome to the person who is going to approve the pull request).
    b. another one for merge/push request(this will be triggered when the pull request is approved i.e, when the merge happens).
For Dev, there won't be any triggers, dev deployments will happen from the feature branch.

<b>git hooks</b>:
In github we don't have option to restrict the new branch creation of they don't match our naming standards. Hence we need to create the git hooks to validate our branch naming standards but the hooks are stored in '.git/hooks' folder and they are not version controlled, meaning the .git folder is not pushed to servver/github. Hence every developer need to create push git hook file in thier local to get it executed by default. Instead of this we moved the scripts to .githooks folder and pushed this folder to github. Additionally user need to change their default hook directory by running this command.
git config core.hooksPath .githooks

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
1. Create folder call infra and app.
2. If tables already exists in the bq, just exit with failure.
3. Logs need to go to cloud logs, not to the bucket.
4. The feature branch should be with right naming conv - feature:DATPAY-2506 otherwise, commit should fail.
5. Document the process.

