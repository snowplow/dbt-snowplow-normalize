# snowplow-normalize-integration-tests

Integration test suite for the snowplow-normalize dbt package.

The `./scripts` directory contains one scripts:

- `integration_tests.sh`: This tests the macros of the snowplow-normalize package. It physically checks the output of the macros to ensure it matches what we expect, but doesn't actually process any data directly.

Run the scripts using:

```bash
bash integration_tests.sh -d {warehouse}
```
