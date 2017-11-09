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
            trips {
              trip_id
              route_id
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
