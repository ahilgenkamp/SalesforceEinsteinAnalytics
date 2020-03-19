# SalesforceEinsteinAnalytics #

Python package for working with the [Einstein Analytics API](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_rest_overview.htm)

* ***What does it do?*** This package allows you to easily perform several operations that are cumbersome in the Einstein Analytics UI.  It allows you to update app access, run SAQL querys for further exploration in Python, restore old versions of a dashboard and to get app access details so that you can review who has access to your data.
* ***Which systems are supported?*** Currently this has only been tested on Windows and with Chrome/Firefox browsers


## Install ##
```
pip3 install SalesforceEinsteinAnalytics
```

## Usage ##

To get started you will need to log into Einstein Analytics in Chrome or Firefox.  This package uses a live session to make API requests.  To create an instance of the function you will need to define your browser and supply an environment URL.
```
EA = salesforceEinsteinAnalytics(env_url='https://yourinstance.my.salesforce.com', browser='chrome')
```

Running a SAQL Query.
For details on running SAQL querys you can find the documentation on the [salesforce developer site.](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_saql.meta/bi_dev_guide_saql/)
```
saql = '''q = load "DatasetAPIName";
				q = filter q by date('data_dt_Year', 'data_dt_Month', 'data_dt_Day') in ["current month".."current month"];
				q = group q by ('dimension1', 'dimension2');
				q = foreach q generate 'dimension1', 'dimension2', sum('metric') as 'metric', unique('id') as 'id_count';
				q = order q by 'metric' desc;
				q = limit q 2000;
	'''

result = EA.run_saql_query(saql=saql)
print(result)
```
