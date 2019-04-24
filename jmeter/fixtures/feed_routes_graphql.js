console.log(
  JSON.stringify({
    query: `
      query ($namespace: String) {
        feed(namespace: $namespace) {
          feed_id
          feed_version
          filename
          routes {
            route_id
            route_type
          }
        }
      }
    `,
    variables: JSON.stringify({
      namespace: "${namespace}"
    })
  })
)
