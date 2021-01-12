# Since the e2e tests take a while to run and it could present an inconvenience
# to be making sure the e2e tests work on every single PR, only run the e2e
# tests on Travis for PRs to master or on commits directly to dev or master
echo $GITHUB_BASE_REF_SLUG
echo $GITHUB_REPOSITORY
echo $GITHUB_SLUG_REF
if [[ "$GITHUB_BASE_REF_SLUG" = "master" ]]; then
  echo "SHOULD_RUN_E2E=true" >> $GITHUB_ENV
  echo 'Will run E2E tests because this is a PR to master'
else
  if [[ "$GITHUB_REPOSITORY" = "ibi-group/datatools-server" ]] && [[ "$GITHUB_SLUG_REF" = "master" || "$GITHUB_SLUG_REF" = "dev" ]]; then
    echo "SHOULD_RUN_E2E=true" >> $GITHUB_ENV
    echo 'Will run E2E tests because this is a commit to master or dev'
  fi
fi

if [[ "$SHOULD_RUN_E2E" != "true" ]]; then
  echo 'Skipping E2E tests...'
fi
