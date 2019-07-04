if [[ "$TRAVIS_REPO_SLUG" = "conveyal/datatools-server" ]] && [[ "$TRAVIS_BRANCH" = "master" || "$TRAVIS_BRANCH" = "dev" ]]; then
    export SHOULD_RUN_E2E=true;
fi

# temporarily set SHOULD_RUN_E2E to true while developing e2e-coverage branch
# remove this before merging to dev
export SHOULD_RUN_E2E=true;