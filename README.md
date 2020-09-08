# SalesforceEinsteinAnalytics #

Python package for working with the [Einstein Analytics API](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_rest_overview.htm)

***NEW FEATURES:*** 
1) Upload data frames to EA as a CSV.  This allows you to do things like run python based machine learning models and upload or update the data in Einstein Analytics for visualization or dashboards.
2) Set up an archive process to keep your apps clean and up-to-date.  There are now functions to add a prefix to dashboard names to warn your users that dashboards or lenses will be archived soon.  There are also functions to get asset metadata to check for dashboards or lenses that have not been updated in a long time.  Lastly, there is a function that allows you to move a list of assets to a designated archive app.

* ***What does it do?*** This package allows you to easily perform several operations that are cumbersome in the Einstein Analytics UI.  It allows you to update app access, run SAQL queries for further exploration in Python, upload data frames, archive assets, and restore old versions of a dashboard and to get app access details so that you can review who has access to your data.
* ***Which systems are supported?*** Currently, this has only been tested on Windows and with Chrome/Firefox browsers


## Install ##
```bash
pip install SalesforceEinsteinAnalytics
```


## Troubleshooting ##
1) **"BrowserCookieError: Failed to find Chrome cookie"**  SalesforceEinsteinAnalytics relies on [browser-cookie3](https://github.com/borisbabic/browser_cookie3) to get the live session cookie and authenticate the session.  This package assumes the standard path for the chrome cookie file.  There may be cases where chrome is set up with a non-standard path.  In this scenario you will need to find the cookie file for your system. Once you find the path you can pass it as a variable to the package init function as shown below.  

```python
cookiefile = 'C:\\Users\\<user_name>\\AppData\\Local\\Google\\Chrome\\User Data\\Profile 1\\Cookies'
EA = salesforceEinsteinAnalytics(env_url='https://yourinstance.my.salesforce.com', browser='chrome', cookiefile=cookiefile)
```

## Usage ##

To get started you will need to log into Einstein Analytics in Chrome or Firefox.  This package uses a live session to make API requests.  To create an instance of the function you will need to define your browser and supply an environment URL.
```python
import SalesforceEinsteinAnalytics as EA
EA = EA.salesforceEinsteinAnalytics(env_url='https://yourinstance.my.salesforce.com', browser='chrome')
```
  
  
Running a SAQL query is simple and allows you to play with data that lives in Einstein Analytics.
For details on running SAQL queries you can find the documentation on the [salesforce developer site.](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_saql.meta/bi_dev_guide_saql/)
```python
saql = '''
q = load "DatasetAPIName";
q = filter q by date('data_dt_Year', 'data_dt_Month', 'data_dt_Day') in ["current month".."current month"];
q = group q by ('dimension1', 'dimension2');
q = foreach q generate 'dimension1', 'dimension2', sum('metric') as 'metric', unique('id') as 'id_count';
q = order q by 'metric' desc;
q = limit q 2000;
'''

result = EA.run_saql_query(saql=saql)
print(result.head())
```
  
The ```load_df_to_EA()``` function allows you to easily load a dataframe to Einstein Analytics.  The simple usage is to pass the dataframe to the function with either the API name of an existing dataset or the new name for your dataset (new datasets will be loaded to your private app). An xmd file will be created using the datatypes from the supplied dataframe. 
```python

df = pd.DataFrame({'key': ['foo', 'bar', 'baz', 'foo'],
                   'value': [1, 2, 3, 5]})

EA.load_df_to_EA(df, "TEST_DATASET", verbose=True)
```
You can also supply your own xmd/metadata file in order to specify things like fiscal offsets.  More information on the metadata format and structure can be found on the [Salesforce developer site.](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_ext_data_format.meta/bi_dev_guide_ext_data_format/bi_ext_data_schema_overview.htm)
```python

df = pd.DataFrame({'key': ['foo', 'bar', 'baz', 'foo'],
                   'value': [1, 2, 3, 5]})

xmd = {
	"fileFormat": {
		"charsetName": "UTF-8",
		"fieldsDelimitedBy": ",",
		"fieldsEnclosedBy": "\"",
		"linesTerminatedBy": "\r\n"
	},
	"objects": [{
		"connector": "CSV",
		"fullyQualifiedName": "TEST_DATASET",
		"label": "TEST_DATASET",
		"name": "TEST_DATASET",
		"fields": [{
			"fullyQualifiedName": "key",
			"name": "key",
			"type": "Text",
			"label": "key"
		}, {
			"fullyQualifiedName": "value",
			"name": "value",
			"type": "Numeric",
			"label": "value",
			"precision": 18,
			"defaultValue": "0",
			"scale": 2,
			"format": "0.0#",
			"decimalSeparator": "."
		}]
	}]
}

EA.load_df_to_EA(df, "TEST_DATASET", xmd=xmd, verbose=True)
```
  
  
You can also get a dataframe of the user permissions for a specific app.  Providing a save_path will save the dataframe as a CSV.  If a save_path is not provided it will just return a dataframe.
```python
app_user_df = EA.get_app_user_list(app_id=['00lXXXXXXXXXXXXXXX'], save_path='C:\\Users\\username\\Documents\\App_User_List.csv')
print(app_user_df.head())

#if no app_id list is added to the function it will return the access list for all apps.
all_apps_user_df = EA.get_app_user_list(save_path='C:\\Users\\username\\Documents\\All_Apps_User_List.csv')
print(all_apps_user_df.head())
```

To help manage dashboards and lenses in apps that you manage there are several functions that may be useful to monitor and archive assets.  Currently, there are three functions that can be used for this purpose:  *getMetaData(), addArchivePrefix(), and archiveAssets().*  These can be used together as in the example below to manage apps and to archive old and unused assets.  To archive assets, it is recommended to create a separate app to store old assets so that it is only accessible by admins.  Additionally, there is a function called *getAssetCounts()* to check the number of assets in each app.

```python
# Example of using meta data to generate a list of assets to archive
df = EA.getMetaData(appIdList=['00lXXXXXXXXXXXXXXX'], objectList=['dashboards','lenses','datasets'], verbose=True)
df = df[(df['lastModifiedDate'] < pd.to_datetime('2019-01-01', utc=True)) & (df['createdBy.name'] == 'John Smith')]
toArchiveList = df['id'].tolist()

# Example of adding archive warning to asset name
import datetime
from dateutil import tz
curr_time = datetime.datetime.utcnow().replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())
warnDate = (curr_time + datetime.timedelta(days=30)).strftime("%m-%d-%Y")

EA.addArchivePrefix(warnList=toArchiveList, prefix='[ARCHIVE ON '+warnDate+'] ', verbose=True)

# Example of moving assets to an archive app
EA.archiveAssets(archiveAppId='00lXXXXXXXXXXXXXXX', ToMoveList=toArchiveList, verbose=True)

# Get asset counts for a list of apps
apps = ['00lXXXXXXXXXXXXXXX','00lYYYYYYYYYYYYYYY','00lZZZZZZZZZZZZZZZ']
df = EA.getAssetCounts(appIdList=apps, countsToReturn=['dashboards','lenses','datasets'], verbose=True)
```
  
To restore a dashboard to a previous version you can use the restore_previous_dashboard_version function and following examples.  The first example will return a dataframe showing the history versions available.  It is generally good to review this file first to view which version you want to restore.  To inspect the JSON of a previous version you can use the second example.  The third example can then be used to revert a dashboard to a previous version.
```python
#View dashboard History
history_df = EA.restore_previous_dashboard_version(dashboard_id='0FKXXXXXXXXXXXXXXX')
history_df.to_csv('C:\\Users\\username\\Documents\\dash_version_history.csv', index=False)

#Get JSON of the previous version to review
EA.restore_previous_dashboard_version(dashboard_id='0FKXXXXXXXXXXXXXXX', version_num=1, save_json_path='C:\\Users\\username\\Documents\\jsonFile.json')

#Restore the previous version of a dashboard
EA.restore_previous_dashboard_version(dashboard_id='0FKXXXXXXXXXXXXXXX', version_num=1)
```
  
  
Lastly, there are functions that you can use to update access in Einstein Analytics apps.  There are 4 different options for updating access.

* **addNewUsers:** This will take the existing users and add the new users provided in the dictionary.
* **removeUsers:** You only need to supply the "sharedWithId: userId" and this will remove those of users.
* **updateUsers:** The input format is the same as what is needed for addNewUsers.  You will just need to change the accessType.
* **fullReplaceAccess:** Dictionary is in the same format as what is passed when adding a new user if you want to replace the entire access.  You will just use fullReplaceAccess as the update_type. ***Be careful with this as it will erase all existing access and only update with what you have included in the user_dict.***

```python
#Example of how to add new users to your app.  
users_to_add = [
			{
				"accessType": "view",
				"shareType": "user",
				"sharedWithId": "005XXXXXXXXXXXXXXX"
			}
		]

EA.update_app_access(user_dict=users_to_add, app_id='00lXXXXXXXXXXXXXXX', update_type='addNewUsers')

#Example of how to remove a user from your app
users_to_remove = [
			{
				"sharedWithId": "005XXXXXXXXXXXXXXX"
			}
		]

EA.update_app_access(user_dict=users_to_remove, app_id='00lXXXXXXXXXXXXXXX', update_type='removeUsers')

#Example of updating access for a user
users_to_update = [
			{
				"accessType": "edit",
				"shareType": "user",
				"sharedWithId": "005XXXXXXXXXXXXXXX"
			}
		]

EA.update_app_access(user_dict=users_to_update, app_id='00lXXXXXXXXXXXXXXX', update_type='updateUsers')
```


