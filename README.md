# shoppalyzer-tool
The code to analyze archived pages from Common Crawl assisted by live crawls.

## Setting up the project on cloud server

#### Cloning the project

Open terminal and log in via <code>ssh -i keyfile root@188.245......</code>

Then run the following commands to get GitHub connected<br>
<code>sudo apt update</code> and
<code>apt install gh</code>.

After some authentication magic via tokens from [GitHub](https://github.com/settings/tokens) and 
<code>gh auth login</code> we can clone the project.

<code>gh repo clone Chaosheld/shoppalyzer-tool</code>

### Installing requirements

Move to project: <code>cd shoppalyzer-tool</code>

Rename the distribution file for the credentials and fill accordingly using 
<code>nano credentials.py.dist</code>.

Then setting up venv and install the requirements:

<code>apt install python3.12-venv</code>

<code>python3 -m venv venv</code>

<code>source venv/bin/activate</code>

<code>pip install -r requirements.txt</code>

### Start the first data job

First make sure to have a recent version: <code>git pull</code>

Now create folder "files > input" and provide a simple csv containing domains to be queried and crawled.