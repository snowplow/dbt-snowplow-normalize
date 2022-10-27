{% docs __snowplow_event_splitting__ %}

{% raw %}

# Snowplow Event Splitting Package

Welcome to the model documentation site for the Snowplow Event Splitting dbt package. The package provides 2 macros and a python script that is used to generate your models; these models provide a table per event type that you specify, and can also produce a thin table of the split events and a user table, for use within downstream ETL tools. 

**For more information, including a detailed operation guide please visit the [Snowplow Docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/).**

*Note this model design doc site is linked to latest release of the package. If you are not using the latest release, [generate and serve](https://docs.getdbt.com/reference/commands/cmd-docs#dbt-docs-serve) the doc site locally for accurate documentation.*

## Overview

This package consists of two macros, a python script, and some example configuration files to help you get started:

  - `split_events` _(macro)_: This macro does the heavy lifting of the package, taking a series of inputs to generate the SQL required to split the events table and flatten (1 level) of any [self-describing event][self-desc-events] or [context][sp-contexts] columns. While you can use this macro manually it is recommended to create the models that use it by using the script provided.

  - `users_table` _(macro)_: This macro takes a series of inputs to generate the SQL that will produce your users table, using the `user_id` column and any custom contexts from your events table.

  - `snowplow_split_events_model_gen.py` _(script)_: This script uses an input configuration to generate your per-event models based on the schemas used to generate those events in the first place. See the [operation docs][splitting-operation] section for more information.
    ```yml
    usage: snowplow_split_events_model_gen.py [-h] [--version] [-v] [--dryRun] [--configHelp] config

    Produce dbt model files for splitting your Snowplow events table into 1 table per event.

    positional arguments:
    config         relative path to your configuration file

    optional arguments:
    -h, --help     show this help message and exit
    --version      Show program's version number and exit.
    -v, --verbose  Verbose flag for the running of the tool
    --dryRun       Flag for a dry run (does not write to files).
    --configHelp   Prints information relating to the structure of the config file.
    ```

  - `example_event_split_config.json`: This file is an example of an input to the python script, showing all options and valid values. For additional information about the file structure run `python utils/snowplow_split_events_model_gen.py --configHelp` in your project root.

  - `example_resolver_config.json`: This file is an example [Iglu Resolver][iglu-resolver] configuration. It supports custom iglu servers with API keys, but does not currently support accessing embedded registries. For more information please see the Resolver docs.



## Installation

Check [dbt Hub](https://hub.getdbt.com/snowplow/snowplow_web/latest/) for the latest installation instructions, or read the [dbt docs][dbt-package-docs] for more information on installing packages.

# Join the Snowplow community

We welcome all ideas, questions and contributions!

For support requests, please use our community support [Discourse][discourse] forum.

If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-web package is Copyright 2021-2022 Snowplow Analytics Ltd.

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
[iglu-resolver]: https://docs.snowplow.io/docs/pipeline-components-and-applications/iglu/iglu-resolver/
[self-desc-events]: https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/#self-describing-events
[sp-contexts]: https://docs.snowplow.io/docs/understanding-tracking-design/predefined-vs-custom-entities/#custom-contexts
[splitting-operation]: https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-event-splitting-model/#operation

{% endraw %}
{% enddocs %}
