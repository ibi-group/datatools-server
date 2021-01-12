# Since the e2e tests take a while to run and it could present an inconvenience
# to be making sure the e2e tests work on every single PR, only run the e2e
# tests on Travis for PRs to master or on commits directly to dev or master
if [[ ! -z "$GITHUB_HEAD_REF" ]]; then
  if [[ "$GITHUB_REF" = "master" ]]; then
    export SHOULD_RUN_E2E=true;
    echo 'Will run E2E tests because this is a PR to master'
  fi
else
  if [[ "$GITHUB_REPOSITORY" = "ibi-group/datatools-server" ]] && [[ "$GITHUB_REF" = "master" || "$GITHUB_REF" = "dev" ]]; then
    export SHOULD_RUN_E2E=true;
    echo 'Will run E2E tests because this is a commit to master or dev'
  fi
fi
