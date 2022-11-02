# ds-dbt

Welcome to Talkspace's dbt project!

## Before you begin

Make sure you've completed the following steps:
- ask the SRE team to create a personal development schema in the Analytics Redshift Cluster, aka ARC. Typically the naming convention is `dbt_<yourname>`; for example, Ringo Starr's would be `dbt_ringo`.
- make sure you have credentials for ARC (username, password, etc.).
- clone the `ds-dbt` repo into your local machine (`git clone https://github.com/talktala/ds-dbt.git`). We recommend cloning the repo as a folder within your home directory.
- check your current version of macOS. If you want to install dbt via `homebrew` instead of `pip`, you may need to update your macOS to the latest version so that the latest version of XCode will install.

## Install dbt via homebrew or pip

First, if you haven't already, [install homebrew](https://brew.sh/). (It theoretically shouldn't matter _where_ on your computer you execute these commands, but your executing from your home directory should work.)

Once you have `homebrew` working, [install dbt](https://docs.getdbt.com/dbt-cli/install/overview) using either `homebrew` or `pip`.

The `homebrew` option:

```
brew update
brew install git
brew tap dbt-labs/dbt
brew install dbt-redshift
```

The `pip` option:

```
pip install dbt-redshift
```

## Configure a profile

Next, configure a profile so that dbt can connect to the database. The following commands will create a `profiles.yml` file in `~/.dbt/`. Starting from the main `ds-dbt` repo directory, input the following commands:

- `cd talkspace`
- `dbt init`

Enter your credentials in the interactive prompts:
- When prompted for a `dbname`, meaning the default database that dbt will build objects in, enter `talktala_production`. This specifically refers to the ARC database named `talktala_production`.
- For the `schema`, enter your development schema: `dbt_<your_first_name>`. This is where your models will be created and where dbt will run code in general.
- When prompted for `threads`, the recommended value per [dbt docs](https://docs.getdbt.com/dbt-cli/configure-your-profile#understanding-threads) is `4`.

When the prompt completes, please edit the `~/.dbt/profiles.yml` file and add the following lines at the top, above the main `talkspace` entry:

```
config:
  send_anonymous_usage_stats: false
```

Connect to the data science VPN so that you can connect to the database.

Run `dbt debug` to confirm that setup was successful.

Run `dbt deps` to download dependencies used in the project.

## Commands

- [dbt run](https://docs.getdbt.com/reference/commands/run)
    - Create models in the database
- [dbt test](https://docs.getdbt.com/reference/commands/test)
    - Run tests against the models
- [dbt deps](https://docs.getdbt.com/reference/commands/deps)
    - Download dependencies from Git
- [dbt docs generate](https://docs.getdbt.com/reference/commands/cmd-docs)
    - Generate documentation for the project (based on `schema.yml` files)
- [dbt docs serve](https://docs.getdbt.com/reference/commands/cmd-docs)
    - Launch a local webserver so you can view the docs in a browser (`Ctrl+C` to terminate the server)

Note: By default, `dbt run` or `dbt test` will create all models or run all tests. If you wish to create or test a single model or subset of models, see [selection syntax](https://docs.getdbt.com/reference/node-selection/syntax).

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
    - See [here](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview) for project structure and file naming
    - See [here](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md#naming-and-field-conventions) for a style guide