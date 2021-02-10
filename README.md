<!---
Copyright © 2020 Hashmap, Inc

Licensed under the Apache License, Version 2.0 the \("License"\);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
--->

# Hashmap Data Cataloger

Table of Contents

- [About](#about)
- [Using Hashmap Data Cataloger (hdc)](#Using-hashmap-data-cataloger)
    - [Setup](#Setup)
    - [Running from CLI](#Running-from-CLI)
    - [Using as API](#Using-as-API)  
- [Future Roadmap](#Future-Roadmap)
- [Notes to developers](#Notes-to-developers)

## About

The Hashmap Data Cataloger utility that can be used to catalog(read) data assets such as Databases, Schemas, and Tables
from a given source system and map(write) them into a given destination system. 


## Using Hashmap Data Cataloger

hashmap-data-cataloger (hdc) is can be invoked from the command line interface (next section) or as a library of APIs.

### Setup

#### Package Installation 
   
This tool is available on PyPi and can be installed as:

```bash
pip install hashmap-data-cataloger
```
    
This will install the hashmap-data-cataloger and all of its dependencies. This is a pypi package and can be installed as


#### Connection configuration file setup

The hdc tool is a configuration driven application that depends on 3 types of configurations encoded as [YAML](https://yaml.org/).

##### Application Configuration

The hdc tool uses this YAML file to configure the 3 primary helper objects that enable the API functions 'map' or 'cataog'.
The elements required in the YAML file and their layout looks like [this](resources/app_config.yml).

You can override this file from CLI using the '-c' option followed by the path of the custom YAML file. 
However, it must conform to the format linked above.

To create a default YAML configuration file do the following:

1. Using any text editor create a file like [this](resources/app_config.yml) and save as 'app_config.yml'
2. Create a hidden directory in the User's root with the name '.hdc'
3. Move the 'app_config.yml' into the hidden directory created above. 



##### Connection Profile Configuration

The hdc tool uses this YAML file to configure/provide the necessary connection details for source and destination databases.
The elements required in the YAML file and their layout looks like [this](resources/profile.yml).
Presently, the connections are secured via user credentials.

You __cannot__ override this file from CLI and therefore will need to be made available beforehand as follows:

1. Using any text editor create a file like [this](resources/profile.yml) and save as 'profile.yml'
2. Create a hidden directory in the User's root with the name '.hdc'
3. Move the 'profile.yml' into the hidden directory created above. 


##### Log Settings Configuration

The hdc tool uses this YAML file to configure the log settings (Python's logging).
The elements required in the YAML file and their layout looks like [this](resources/log_settings.yml).

You can override this file from CLI using the '-l' option followed by the path of the custom YAML file. 
However, it must conform to the format linked above.

To create a default YAML configuration file do the following:

1. Using any text editor create a file like [this](resources/log_settings.yml) and save as 'log_settings.yml'
2. Create a hidden directory in the User's root with the name '.hdc'
3. Move the 'log_settings.yml' into the hidden directory created above. 


###  Running from CLI
Once the package is installed along-with its dependencies, invoke it from the command line as:

```bash
usage: hdc [-h] [-c APP_CONFIG] [-l LOG_SETTINGS] -r {catalog,map} -s SOURCE
           [-d DESTINATION] [-m MAPPER]

optional arguments:
  -h, --help            show this help message and exit
  -c APP_CONFIG, --app_config APP_CONFIG
                        Path to application config (YAML) file if other than default
  -l LOG_SETTINGS, --log_settings LOG_SETTINGS
                        Path to log settings (YAML) file if other than default
  -r {catalog,map}, --run {catalog,map}
                        One of 'catalog' or 'map'
  -s SOURCE, --source SOURCE
                        Name of any one of crawlers configured in app_config.yml
  -d DESTINATION, --destination DESTINATION
                        Name of any one of creators configured in app_config.yml
  -m MAPPER, --mapper MAPPER
                        Name of any one of mappers configured in app_config.yml
```

### Using as API 

Other applications could import hdc as a library and make use of the cataloging or mapping functions as explained below.

   > 1. __AssetMapper.map_assets()__ - Map the data assets from a source system to a target system. 
                                  AssetMapper class requires 3 keyword arguments -'source', 'destination', and 'mapper'
                                  which are names configured under 'crawlers', 'creators', and 'mappers' resp,
                                  in the app_config.yml file. 
                                  For more information, get help on *hdc.core.asset_mapper.AssetMapper* class
   >                               
   > 2. __Cataloger.obtain_catalog()__ - Crawls a given source system and pulls the data asset information to return as a tuple.
                                   Cataloger class requires 1 keyword argument -'source', 
                                   which is the name of a configured 'crawlers' in the app_config.yml file. 
                                   For more information, get help on *hdc.core.cataloger.Cataloger* class
           
                                     

## Future Roadmap

### Using external Key Store 

#### For enhanced user authentication 

Allow configuration of external Key Stores for storing user authentication details required while connecting with source or destination systems. 
The application shall be able to interact with the external KS based on the configuration provided. 

This is to provide a stronger security option instead of directly configuring the user credentials in the profile.yml file. 


## Notes to developers

### OOP Design
[UML Class Diagram](https://lucid.app/lucidchart/invitations/accept/357e8f4a-b943-4fbe-a488-57d75342a17b)

### Extending capability

#### Adding new crawler

TBD

#### Adding new mapper

TBD

#### Adding new creator

TBD