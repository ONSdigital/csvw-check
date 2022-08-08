Validate CSV-W based on tests provided by W3C (https://w3c.github.io/csvw/tests/#manifest-validation)


# Development

To run all tests with docker:

- 1. Build the docker image `docker build -t csvwcheck .`
- 2. Run the tests with the image `docker run -v $PWD:/workspace -w /workspace -it csvwcheck /bin/bash -c "sbt test"`

To run a _specific_ test

- Comment out this class https://github.com/GSS-Cogs/csvw-check/blob/c657b76a68da0dc4baf29c11d17694777379ca15/src/test/scala/csvwcheck/RunCucumberTest.scala#L7
- _Uncomment_ this class https://github.com/GSS-Cogs/csvw-check/blob/c657b76a68da0dc4baf29c11d17694777379ca15/src/test/scala/csvwcheck/RunCucumberTest.scala#L11
- Decorate the scenarios you are working on with `@runThisTest`