
===== Usage of ES Diagnotics tool =====


=== Setup of Tool in Client VM

1. Pick any VM where you can install the tool and run it. It can be your laptop also.
2. Follow below steps to download the tool and install it.

#Clone the repo

cd ESDiagnostics

#Make sure python 3.6+ exists
python3 --version 

#Install the required python libraries
sudo python3 -m pip install -r ./required_packages.txt

=== Collecting the stats from ES VM

1. Login to ES VM
2. Create getStatsFromES.sh using the repo folder you clones in last setp
3. Run below command to collect the stats

  sh ./getStatsFromES.sh "ES URL" "Username" "Password"
    example:  sh ./getStatsFromES.sh  "https://100.104.145.145:9200" "elastic" "K3RUJN8uLw"

4. The above command will create a stats zip under /tmp

5. Copy that zip to your client VM

=== Running the tool from Client VM

cd ESDiagnostics
python3 es_search.py -StatsZipFile ESDiagnosticsDump.zip

