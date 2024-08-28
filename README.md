# ES Diagnostics Tool Usage

## Setup on Client VM

1. Choose any VM where you can install and run the tool. This can also be your laptop.
2. Follow the steps below to download and install the tool.

### Clone the Repository

```bash
git clone https://github.com/bbcf13/ESDiagnostics.git
cd ESDiagnostics
```

### Ensure Python 3.6+ is Installed

```bash
python3 --version
```

### Install Required Python Libraries

```bash
sudo python3 -m pip install -r ./required_packages.txt
```

## Collecting Stats from ES VM

1. Log in to the ES VM.
2. Create the `getStatsFromES.sh` script using the repo folder you cloned in the last step.
3. Run the following command to collect the stats:

```bash
sh ./getStatsFromES.sh "ES_URL" "Username" "Password"
```

**Example:**

```bash
sh ./getStatsFromES.sh "https://100.104.145.145:9200" "elastic" "K3RUJN8uLw"
```

4. The command above will create a stats zip file under `/tmp`.
5. Copy the zip file to your client VM.

## Running the Tool from Client VM

```bash
cd ESDiagnostics
python3 es_search.py -StatsZipFile ESDiagnosticsDump.zip
```