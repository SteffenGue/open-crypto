# Introduction to the Data Collector _open-crypto_

<img src="https://user-images.githubusercontent.com/65443847/129176307-200b1554-0f3c-4e77-81ad-71f5f884c6cd.png" width="600" height="450">

This is a short introduction to the package called __open-crypto__. The program offers generalized _REST-API requests_ to over __100__ of the largest and most liquid __cryptocurrency exchanges__ and several well-known __platforms__. To this point, the program can request (see examples below):
- __Ticker__ data
- __Transaction__ data
- __Historical-Rate__ data
- __Order-Book__ data
- __Aggreg. Platform__ data

We offer an open-source tool which fits the needs for unprocessed and aggregated data in a new, fast, flexible and in many cases unexplored industry.
Along with this short introduction we provide several examples to execute listed at the end.

## Prerequisites
- [Python >= 3.8](<https://www.python.org/downloads/>)

## Getting started
The program is uploaded to __PyPI__. For installation, execute:
```shell
pip install open-crypto
```
in your command prompt. Ensure to set the Python executable as path variable. This can be selected during the installation process.

### Dependencies
The following third-party libraries are installed with __open-crypto__:
- aiohttp
- aioschedule
- certifi
- validators
- pytest
- oyaml
- tqdm
- matplotlib
- pandas
- datetime_periods
- python-dateutil
- sqlalchemy
- sqlalchemy_utils
- nest-asyncio
- typeguard
- colorama

## Run the program

The program is initialized using a configuration file. In order to keep things simple, we offer several exemplary configurations, one for each request method.

In order to make adjustments, all files and collected data will be copied to your current working directory, including the configuration, exchange mappings, log files and database(s). Therefore run the module in the desired directory.

The module ```runner``` offers several functionalities to control the program:
```python
>>>runner.check_path() # check if resources are in your working directory.
>>>runner.update_maps() # download the lastest exchanges mappings from the GitHub repository
>>>runner.copy_resources() # copy the resources into the currency working directory
>>>runner.get_session() # return an open database connection.
>>>runner.exchanges_and_methods() # return all implemented exchanges and their supported API endpoints.
>>>runner.get_config() # print a specified or the actual configuration file
>>>runner.get_config_template() # return an empty configuration file to the resource directory.
>>>runner.export() # allow exporting data from the database into csv/hdf-files.
>>>runner.run() # start the program.
```
For more details, make use of the _help_ function:
```python
>>>help(runner)
```
To start the data collector, open ```Python``` within your working directory of choice and import the program:
```python
>>>from open_crypto import runner
>>>runner.update_maps()
Updating exchange mappings from GitHub.. 100%
```
The first command will import the module ```runner```. Within ```runner``` the function ```update_maps``` will download the latest exchange mappings from GitHub and (if the folder already exists overwrite) all ```resources``` into your working directory.

Personalized requests can be made by first creating a new configuration file template:
```python
>>>runner.get_config_template()
Created new config template.
```
The file can be found under: ```[your/cwd]/resources/configs/user_configs/request_template.yaml```. Open and manipulate the file with a text editor of choice. Consider renaming the file accordingly.

Finally, read in the file and execute the program:
```python
>>>from open_crypto import runner
>>>runner.run()
Enter config file name: <your_file_name>
```
For a first impression, consider executing the following examples before creating personalized tasks.

## Examples
By default, several example scripts are offered and can easily be executed:
- __static()__   
- __exchange_listings()__            
- __trades()__             
- __order_books()__        
- __platform()__           
- __minute_candles()__     

To run __open_crypto__ with one of the mentioned configuration files:
```python
>>>from open_crypto import runner
>>>runner.Examples.minute_candles()
```
Note that all examples will result in a plot of the received data. Furthermore, especially _static_ and _exchange_listings_ may take several minutes.

## Valid requests
The following will present the configuration file and show some possible valid requests which should serve as a template to create your own.
The file is structured into ```general``` settings, including ```database``` and ```operation_settings```. Furthermore, the section ```jobs``` lists all important parameters for the request itself:

```yaml
general:
  database:
    sqltype: sqlite #sqlite, mariadb, mysql or postgres
    client: null # mariadb, pymysql or psycopg2
    user_name:
    password:
    host: localhost
    port: 5432
    db_name: ExampleDB

  operation_settings:
    frequency: once # once or any number in minutes (i.e. 0.1 for 6 sec)
    interval: days # minutes, hours, days, weeks, months
    timeout: 10 # any number in seconds (max response time for an exchange)
    enable_logging: true
    asynchronously: true # run in parallel or iteratively request currency-pairs
    
jobs:
  JobName:  # An arbitrarily chosen name
    yaml_request_name: historic_rates # ticker, trades, order_books, historic_rates
    update_cp: false
    excluded: null # comma-separated list of exchange names
    
    exchanges: # all or an comma-separated list of exchange names
    currency_pairs: # all or an comma-separated list of currency-pairs (e.g. eth-btc, btc-usd, ..)
    first_currencies: null # comma-separated list of currencies (e.g. eth, btc, ..)
    second_currencies: null 
```

Leaving all ```general``` settings untouched will save the requested data into a ```SQLite``` database within your current working directory. A valid request, simply catching (daily) historical candles from ```Coinbase``` and ```Bitfinex``` for the currency-pair ```btc-usd```, looks like the following:

```yaml
general:
  database: <...>
  operation_settings: <...>
 
jobs:
  Example:  
    request_method: historic_rates
    update_cp: false
    excluded: null
    exchanges: coinbase, bitfinex
    currency_pairs: btc-usd
    first_currencies: null
    second_currencies: null
```
Note that the request interval (```minutes```, ```days```, ...) and frequency (only ```once``` and not iteratively) is listed under ```operation_settings```. Further currency-pairs can easily be appended in the same format. However, if one is interested in catching all currency-pairs from the same exchanges with base-currency Bitcoin (i.e. ```btc-usd```, ```btc-eur```, ```...```):
```yaml
general:
  database: <...>
  operation_settings: <...>
 
jobs:
  Example:  
    request_method: historic_rates
    update_cp: false
    excluded: null
    exchanges: coinbase, bitfinex
    currency_pairs: null
    first_currencies: btc
    second_currencies: null
```
Note, when applying a further filter for US-Dollar to ```second_currencies```, both former requests are identical. Lastly, one may be interested in all historical ```btc-usd``` time series available, therefore:

```yaml
general:
  database: <...>
  operation_settings: <...>
 
jobs:
  Example:  
    request_method: historic_rates
    update_cp: false
    excluded: null
    exchanges: all
    currency_pairs: btc-usd
    first_currencies: null
    second_currencies: null
```

## Invalid requests
In order to provide users feedback when specifying invalid request configurations, __open-crypto__ validates the file before starting requests. The following will show examples of misspecifications and the respective response from the program. 

Let's first create an empty configuration file and plug it in (see above). Note that neither the request_method, exchanges nor currency-pairs are selected.
```python
>>>runner.get_config_template()
Created new config template.
>>>runner.run()
Enter config file name: request_template
+ Load file was valid.
+ YAML Parsing successful.
+ Configuration contains all blocks: ['general', 'jobs'] and sections: ['database', 'operation_settings']
+ Database connection string is valid
+ Operation_settings have valid keys
+ Operation settings are valid
- Key request_method: Expected type(s) '<class 'str'>' != actual type '<class 'NoneType'>'.
```
(+) symbolizes a positive result, whereas (-) symbolizes an error. In this case, the key __request_method__ is of class ```NoneType```, whereas the program expected a ```String```. __open-crypto__ stops validating after an error.

Let's step further and take the following config file, where the currency-pair is falsely specified:
```yaml
general:
  database: <...>
  operation_settings: <...>
  utilities: <...>
 
jobs:
  Example:  
    yaml_request_name: historic_rates
    update_cp: false
    excluded: null
    exchanges: all
    currency_pairs: btc/usd
    first_currencies: null
    second_currencies: null
```
will result in the follwing error message indicating that only the splitting values (-) between currencies and (,) between currency-pairs are allowed:
```python
>>>runner.run()
Enter config file name: request_template
+ Load file was valid.
<...>
- Expected splitting value(s) '['-', ',']' != actual value 'btc/usd' in 'currency_pairs'.
```
Last, let's try a request without specifying any currency-pair, i.e.:
```yaml
general:
  database: <...>
  operation_settings: <...>
  utilities: <...>
 
jobs:
  Example:  
    yaml_request_name: historic_rates
    update_cp: false
    excluded: null
    exchanges: all
    currency_pairs: null
    first_currencies: null
    second_currencies: null
```
will result in the following response:
```python
>>>runner.run()
Enter config file name: request_template
+ Load file was valid.
<...>
- Expected one key(s) '['currency_pairs', 'first_currencies', 'second_currencies']' != None.
```

## Troubleshooting
- If you are running on MacOS and do not receive any data, it is likely that ```Python``` does not have access to your root ssl-certificate. In that case, try executing the following file: ```applications/Python [your/version]/Install Certificates.command```.
- Some ```IPython``` distributions, used for example by ```Spyder``` or ```Jupyter Notebook```, run within an ```asyncio.BaseEventLoop```. Unfortunately, ```asyncio``` is not supportive of nested event-loops, therefore raising an ```RuntimeError``` when executing __open-crypto__. We applied a patch, namely the package ```nest-asyncio``` which should cover most distributions. However, if the error still remains, consider changing the IDE (e.g. PyCharm) or run __open-crypto__ with plain ```Python``` in your terminal.


