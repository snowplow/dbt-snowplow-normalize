[![early-release]][tracker-classification] [![Release][release-image]][releases] [![License][license-image]][license] [![Discourse posts][discourse-image]][discourse]

![snowplow-logo](https://raw.githubusercontent.com/snowplow/dbt-snowplow-utils/main/assets/snowplow_logo.png)

# snowplow-normalize

This dbt package:

- Provides macros to simplify the production of models that normalize Snowplow event data into a table per event type, plus a reduced events table and latest user context table, for easier integration with downstream tools.
- Includes a python script to generate the models using a simple configuration file that uses the schemas used within your pipelines to identify the columns, to greatly reduce the upfront effort in creating these models.

Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/) for a full breakdown of the package.

### Adapter Support

The snowplow-normalize v0.2.2 package currently supports BigQuery, Databricks & Snowflake.

| Warehouse                        | dbt versions      | snowplow-normalize version |
| :------------------------------: | :---------------: | :--------------------------: |
| BigQuery, Databricks & Snowflake | >=1.3.0 to <2.0.0 | 0.2.2                        |
| BigQuery, Databricks & Snowflake | >=1.0.0 to <2.0.0 | 0.1.0                        |


### Requirements

- A dataset of Snowplow events must be available in the database.

### Installation

Check dbt Hub for the latest installation instructions, or read the [dbt docs][dbt-package-docs] for more information on installing packages.

### Configuration & Operation

Please refer to the [doc site][snowplow-normalize-docs] for details on how to configure and run the package.

### Models

This package only contains the incremental models needed to efficiently process new events for your models, see the docs on [incremental logic](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-advanced-usage/dbt-incremental-logic/) for more information. The package does however produce 3 types of models:

| Model                     | Description                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------ |
| event_name                | A table for each of your specified event names, with flat columns from self-describing events and contexts. |
| filtered_events           | A table that contains the `event_id`s, `collector_tstamp`, event name, and the name of the table that those events have been normalized into. Note it doesn't contain events not split out into individual tables. |
| event_users               | A table with the latest user context columns for any user_ids in your events table.  |

For more detailed information, see the [doc site][snowplow-normalize-docs].
# Join the Snowplow community

We welcome all ideas, questions and contributions!

For support requests, please use our community support [Discourse][discourse] forum.

If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-normalize package is Copyright 2022-2023 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[tracker-classification]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/tracker-maintenance-classification/
[early-release]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Early%20Release&color=014477&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[tracker-docs]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/
[dbt-package-docs]: https://docs.getdbt.com/docs/building-a-dbt-project/package-management
[discourse-image]: https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscourse.snowplow.io%2F
[discourse]: http://discourse.snowplow.io/
[snowplow-normalize-docs]: https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-normalize-data-model/

[release-image]: https://img.shields.io/github/v/release/snowplow/dbt-snowplow-normalize?sort=semver
[releases]: https://github.com/snowplow/dbt-snowplow-normalize/releases
