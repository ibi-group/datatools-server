console.log(
  JSON.stringify({
    query: `
      query ($namespace: String, $route_id: String) {
        feed(namespace: $namespace) {
          feed_id
          feed_version
          filename
          routes (route_id: [$route_id]) {
            route_id
            route_type
            patterns {
              pattern_id
              route_id
              trips {
                trip_id
                pattern_id
              }
            }
          }
        }
      }
    `,
    variables: JSON.stringify({
      namespace: "${namespace}",
      route_id: "${randomRouteId}"
    })
  })
)
