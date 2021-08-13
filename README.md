# Introduction to the Data Collector _open_crypto_
This is a short introcution to the package called __open-crypto__. 


<img src="https://user-images.githubusercontent.com/65443847/129176307-200b1554-0f3c-4e77-81ad-71f5f884c6cd.png" width="600" height="450">

The program offers generalized _REST API requests_ to over __100__ of the largest and most liquid __cryptocurrency exchanges__ and several well-known __platforms__. To this point, the program can request(see examples below):
- __Ticker__ data
- __Transaction__ data
- __Historical-Rate__ data
- __Order-Book__ data
- __Aggreg. Platform__ data

The program is highly customizable and not necessarily limited to cryptocurrency data.
We offer an open-source tool which fits the needs for a new, fast, flexible and in many cases unexplored industry. 
Along with this short introduction we provide an in-depth __Handbook__ (forthcoming) with detailed information about the code, selected points to alter or augment the functionality.
## Prerequisits
- [Python >= 3.9](<https://www.python.org/downloads/>)

## Getting started
The program is uploaded to __PyPI__. For installation, execute:
```shell
pip install open-crypto
```
in your command promt. Please ensure to set the Python executable as path variable. This can be selected during the installation process.

### Dependencies
The following third-party libraries are installed with __open_crypto__:
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

## Run the program

The program is initialized using a configuration file. In order to keep things simple, we offer several exemplary configurations, one for each request method. How to set up the configuration file for customized purposes is described in detail in the Handbook (forthcoming).

In order to make adjustments, all files and collected data will be copied to your current working directory, inlcuding the configuration, exchange mappings, loggings and database. Therefore execute the module in the desired location.

The module ```runner``` offers several functionalities to controll the program:
```python
>>>runner.check_path() # check if resources are in your working directory. If not, call run.update_maps()
>>>runner.update_maps() # copies (and overrides) all files (exchange maps and configurations) to your cwd.
>>>runner.get_session() # returns an open database connection.
>>>runner.exchanges_and_methods() # returns all implemented exchanges and their supported API endpoints.
>>>runner.get_config() # prints a specified or the acutal configuration file
>>>runner.get_config_template() # returns an empty configuration file to the resource directory.
>>>runner.export() # allows to export data from the database into csv/hdf-files.
>>>runner.run() # starts the programm.
```
For more details, make use of the _help_ function:
```python
>>>help(runner)
```
To start the data collector, open ```Python``` within your working directory of choice and import the program:
```python
>>>from open_crpyto import runner
>>>run.update_maps()
Copying resources to [your/current/cwd]..
```
The first command will import the module ```run```. Within ```run``` is the function ```update_mapping``` which will copy (or update if already exist) all ```resources``` into your working directory. The current ```working directory``` can be displayed with ```run.get_path()``` .

Within the directory ```resources``` all important configurations can be made. To keep things simple, we start with the predefined configurations.
Open a command promt or write a ```Python``` script:
```python
>>>from open_crypto import runner
>>>runner.run()
Enter config file name: 

````
and type in the name of the configuration file.


## Examples
By default, several example skripts are offered and can easily be executed:
- __exchange_listings()__  # plots the amount of listings on exchanges over time for the top 10 cryptos
- __static()__             # histogram of currency-pairs over exchanges
- __trades()__             # queries the most recent 1,000 transactions from Coinbase and plots them
- __order_books()__        # queries the most recent order-book snapshot and plots the market-depth
- __platform()__           # queries the price/volume/mcap series for Bitcoin and plots it 
- __minute_candles()__     # queries minute candles for 60 seconds and plots the series

To run __open_crypto__ with one of the mentioned configuration files:
```python
>>>from open_crypto import runner
>>>runner.Examples.minute_candles()
```
Note that all examples will result in a plot of the received data. Furthermore, especially _static_, _exchange_listings_ may take several minutes.