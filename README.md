# Introduction to the Data Collector _open-crypto_

<img src="https://user-images.githubusercontent.com/65443847/129176307-200b1554-0f3c-4e77-81ad-71f5f884c6cd.png" width="600" height="450">

This is a short introduction to the package called __open-crypto__. The program offers generalized _REST-API requests_ to over __100__ of the largest and most liquid __cryptocurrency exchanges__ and several well-known __platforms__. To this point, the program can request (see examples below):
- __Ticker__ data
- __Transaction__ data
- __Historical-Rate__ data
- __Order-Book__ data
- __Aggreg. Platform__ data

We offer an open-source tool which fits the needs for unprocessed and aggregated data in a new, fast, flexible and in many cases unexplored industry.
Along with this short introduction we provide an in-depth __Handbook__ (forthcoming) with detailed information about the code and several examples to execute listed at the end.

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

## Run the program

The program is initialized using a configuration file. In order to keep things simple, we offer several exemplary configurations, one for each request method.

In order to make adjustments, all files and collected data will be copied to your current working directory, including the configuration, exchange mappings, log files and database(s). Therefore run the module in the desired directory.

The module ```runner``` offers several functionalities to control the program:
```python
>>>runner.check_path() # check if resources are in your working directory.
>>>runner.update_maps() # copies (and overrides) all files (exchange maps and configurations) to your cwd.
>>>runner.get_session() # returns an open database connection.
>>>runner.exchanges_and_methods() # returns all implemented exchanges and their supported API endpoints.
>>>runner.get_config() # prints a specified or the actual configuration file
>>>runner.get_config_template() # returns an empty configuration file to the resource directory.
>>>runner.export() # allows exporting data from the database into csv/hdf-files.
>>>runner.run() # starts the program.
```
For more details, make use of the _help_ function:
```python
>>>help(runner)
```
To start the data collector, open ```Python``` within your working directory of choice and import the program:
```python
>>>from open_crypto import runner
>>>runner.update_maps()
Copying resources to [your/current/cwd]..
```
The first command will import the module ```runner```. Within ```runner``` the function ```update_maps``` will copy (or update if it already exists) all ```resources``` into your working directory.

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
Enter config file name: [your/file/name]
```
For a first impression, consider executing the following examples before creating personalized tasks.

## Examples
By default, several example scripts are offered and can easily be executed:
- __exchange_listings()__  
- __static()__             
- __trades()__             
- __order_books()__        
- __platform()__           
- __minute_candles()__     

To run __open_crypto__ with one of the mentioned configuration files:
```python
>>>from open_crypto import runner
>>>runner.Examples.minute_candles()
```
Note that all examples will result in a plot of the received data. Furthermore, especially _static_, _exchange_listings_ may take several minutes.

## Troubleshooting
- If you are running on MacOS and do not receive any data, it is likely that ```Python``` does not have access to your root ssl-certificate. In that case, try executing the following file: ```applications/Python [your/version]/Install Certificates.command```.
- Some ```IPython``` distributions, used for example by ```Spyder``` or ```Jupyter Notebook```, run within an ```asyncio.BaseEventLoop```. Unfortunately, ```asyncio``` is not supportive of nested event-loops, therefore raising an ```RuntimeError``` when executing __open-crypto__. We applied a patch, namely the package ```nest-asyncio``` which should cover most distributions. However, if the error still remains, consider changing the IDE (e.g. PyCharm) or run __open-crypto__ with plain ```Python``` in your terminal.
