# Since the e2e tests take a while to run and it could present an inconvenience
# to be making sure the e2e tests work on every single PR, only run the e2e
# tests on Travis for PRs to master or on commits directly to dev or master
if [[ "$TRAVIS_PULL_REQUEST" != "false" ]]; then
  if [[ "$TRAVIS_BRANCH" = "master" ]]; then
    export SHOULD_RUN_E2E=true;
    echo 'Will run E2E tests because this is a PR to master'
  fi
else
  if [[ "$TRAVIS_REPO_SLUG" = "ibi-group/datatools-server" ]] && [[ "$TRAVIS_BRANCH" = "master" || "$TRAVIS_BRANCH" = "dev" ]]; then
    export SHOULD_RUN_E2E=true;
    echo 'Will run E2E tests because this is a commit to master or dev'
  fi
fi
