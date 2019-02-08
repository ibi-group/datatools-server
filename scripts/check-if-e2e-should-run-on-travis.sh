if [[ "$TRAVIS_PULL_REQUEST" = "true" ]]; then
  if [[ "$TRAVIS_PULL_REQUEST_SLUG" = "conveyal/datatools-server" ]]; then
    export SHOULD_RUN_E2E=true;
  fi
else
  if [[ "$TRAVIS_REPO_SLUG" = "conveyal/datatools-server" ]] && [[ "$TRAVIS_BRANCH" = "master" || "$TRAVIS_BRANCH" = "dev" ]]; then
    export SHOULD_RUN_E2E=true;
  fi
fi
