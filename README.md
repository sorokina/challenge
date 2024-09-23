# GitHub Reporting Pipeline

This repository contains a pipeline to fetch data from the GitHub REST API and create a simple report.

The report compares the developer communities of the Python clients for three lakehouse table formats:
* Apache Hudi ([hudi-rs](https://github.com/apache/hudi-rs))
* Apache Iceberg ([iceberg-python](https://github.com/apache/iceberg-python))
* Delta Lake ([delta-rs](https://github.com/delta-io/delta-rs))

The report has the following format (example data as of 2024-09-16):
|                                 | delta-rs | iceberg-python | hudi-rs |
|---------------------------------|---------:|---------------:|--------:|
| stars                           | 2153     | 383            | 136     |
| forks                           | 384      | 140            | 28      |
| watchers                        | 37       | 29             | 18      |
| releases                        | 73       | 5              | 1       |
| open issues                     | 209      | 123            | 26      |
| closed issues                   | 879      | 226            | 24      |
| avg days until issue was closed | 122.1    | 45.6           | 10.6    |
| open PRs                        | 19       | 54             | 10      |
| closed PRs                      | 1676     | 774            | 83      |
| avg days until PR was closed    | 8.8      | 6.1            | 4.2     |
> *Note*: The avg days KPIs should be rounded to one decimal place. The average days, how long an issue or PR was open, should be calculated by subtracting the field `created_at` from `closed_at` per item and calculate the overall mean value.


## Getting Started

> Poetry is used for managing this repository. You need to have [`poetry`](https://pypi.org/project/poetry/) installed to contribute to this codebase.
>
> Using `pipx` to install Poetry is recommended, because it avoids package version conflicts (see the [pipx docs](https://pipx.pypa.io/stable/)):
> ```bash
> pipx install poetry
> ```
> Otherwise using `pip` is okay as well: (but if package version conflicts occur, `pipx` would be the easiest way around)
> ```bash
> pip install poetry
> ```

1. **Have a local clone of this repository.**

2. **Switch to the root directory of the repository.**
```bash
cd github-reporting/
```

2. **Copy the `.env.template` file, save it as `.env` and and set values:**
```bash
cp .env.template .env
```
Open the `.env` file and set the correct environment variables for the user and group id.

Later, you could add a GitHub API access token as well. For running the job for the first time,
the rate-limiting of the unauthenticated access is enough (60 calls per hour).
For creating a GitHub token, you need a GitHub account.
Visit this page to create a personal access token: https://github.com/settings/tokens

3. **Install virtual environment:**
```bash
poetry install --sync
```

4. **Setup MinIO (bucket `data-lake-local` will be created automatically):**

Create a local folder for MinIO:
```bash
mkdir -p data/minio
```
Start MinIO:
```bash
docker compose up
```
When you see this message, the setup is completed:

> Bucket created successfully \`s3/data-lake-local\`.

Press `Ctrl`+`C` to terminate MinIO after the setup is completed.


## Development

1. **Start MinIO:**
```bash
docker compose up -d
```

2. **Start the Dagster Web UI:**

**If you use VS Code,** you could start the web server in debugging mode (e.g. by pressing `F5`):
Open http://localhost:3000 with your browser to see the project.
You could set breakpoints in the code, which are respected, when you execute jobs in the Dagster Web UI.
When you click the stop icon in the debugging menu bar or press `Shift`+`F5`, you would stop the debugging mode fully, which terminates the Dagster server.
Therefore, you could just leave the debugging bar untouched, during development without stopping the debugging process in the background.

**If you don't use VS Code or prefer to run Dagster without debugging,** you could start the web server with:
```bash
dagster dev
```
Open http://localhost:3000 in your browser to see the project.

3. **Reload Code:**

After you have changed code (both in debugging mode or normal mode), you need to reload the code location in the Web UI, by opening the navigation bar on the left side and clicking on the reload icon at the bottom of the side bar.
Then, you could open the job or asset in the Web UI, which you would like to execute and click "Materialize".

4. **Open MinIO:**

To check the files in the data lake, you could open the MinIO Web UI under http://localhost:9090 (login with user `minio` and password `password`).
Click in the navigation bar on the left on "Object Browser" and open the data lake bucket.  
Or (if you have the AWS CLI installed) run:
```bash
export AWS_ACCESS_KEY_ID=minio && export AWS_SECRET_ACCESS_KEY=password
aws s3 --endpoint-url http://localhost:9000 ls --recursive s3://data-lake-local
```

5. **Stopping the services:**

**To stop MinIO**, run:
```bash
docker compose down
```

**To stop the Dagster Web UI**:

*For debugging mode in VS Code:* 
Terminate the debugging mode and stop the Dagster webserver by pressing `Shift`+`F5`.

*When you have started Dagster in the shell:* 
Press `Ctrl`+`C` in the terminal window, where the dagster process was started.
