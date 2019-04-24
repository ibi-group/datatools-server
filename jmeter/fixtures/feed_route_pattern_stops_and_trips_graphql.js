console.log(
  JSON.stringify({
    query: `
      query ($namespace: String, $pattern_id: String) {
        feed(namespace: $namespace) {
          feed_id
          feed_version
          filename
          patterns (pattern_id: [$pattern_id]) {
            pattern_id
            route_id
            stops {
              stop_id
            }
            trips {
              trip_id
              pattern_id
              stop_times {
                stop_id
                trip_id
              }
            }
          }
        }
      }
    `,
    variables: JSON.stringify({
      namespace: "${namespace}",
      pattern_id: "${randomPatternId}"
    })
  })
)
