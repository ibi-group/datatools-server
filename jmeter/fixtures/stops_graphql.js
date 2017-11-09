console.log(
  JSON.stringify({
    query: `
      query stops($namespace: String) {
        feed(namespace: $namespace) {
          namespace
          feed_id
          feed_version
          filename
          row_counts {
            stops
          }
          stops {
            stop_id
            stop_name
            stop_lat
            stop_lon
          }
        }
      }
    `,
    variables: JSON.stringify({
      namespace: "${namespace}"
    })
  })
)
