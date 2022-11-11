{% docs __snowplow_event_splitting__ %}

{% raw %}

# Snowplow Event Splitting Package

Welcome to the model documentation site for the Snowplow Event Splitting dbt package. The package provides 2 macros and a python script that is used to generate your models; these models provide a table per event type that you specify, and can also produce a thin table of the split events and a user table, for use within downstream ETL tools. 

**For more information, including a detailed operation guide please visit the [Snowplow Docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/).**

*Note this model design doc site is linked to latest release of the package. If you are not using the latest release, [generate and serve](https://docs.getdbt.com/reference/commands/cmd-docs#dbt-docs-serve) the doc site locally for accurate documentation.*

## Overview

This package contains a python script that is designed to be used in conjunction with the macros provided to generate models that decompose the Snowplow `atomic.events` table into individual tables. For more information on usage and the design of the models produced, see the [Package Docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-event-splitting-model).

## Installation

Check [dbt Hub](https://hub.getdbt.com/snowplow/snowplow_event_splitting/latest/) for the latest installation instructions, or read the [dbt docs][dbt-package-docs] for more information on installing packages.

# Join the Snowplow community

We welcome all ideas, questions and contributions!

For support requests, please use our community support [Discourse][discourse] forum.

If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-web package is Copyright 2022 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
[dbt-package-docs]: https://docs.getdbt.com/docs/building-a-dbt-project/package-management
[discourse]: http://discourse.snowplow.io/


{% endraw %}
{% enddocs %}
