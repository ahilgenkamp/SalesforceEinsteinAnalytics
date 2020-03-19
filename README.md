# SalesforceEinsteinAnalytics #

Python package for working with the [Einstein Analytics API](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_rest_overview.htm)

* ***What does it do?*** This package allows you to easily perform several operations that are cumbersome in the Einstein Analytics UI.  It allows you to update app access, run SAQL queries for further exploration in Python, restore old versions of a dashboard and to get app access details so that you can review who has access to your data.
* ***Which systems are supported?*** Currently, this has only been tested on Windows and with Chrome/Firefox browsers


## Install ##
```bash
pip install SalesforceEinsteinAnalytics
```

## Usage ##

To get started you will need to log into Einstein Analytics in Chrome or Firefox.  This package uses a live session to make API requests.  To create an instance of the function you will need to define your browser and supply an environment URL.
```python
import SalesforceEinsteinAnalytics as EA
EA = EA.salesforceEinsteinAnalytics(env_url='https://yourinstance.my.salesforce.com', browser='chrome')
```
  
  
Running a SAQL Query.
For details on running SAQL querys you can find the documentation on the [salesforce developer site.](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_saql.meta/bi_dev_guide_saql/)
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
  
  
You can also get a dataframe of the user permissions for a specific app.  Providing a save_path will save the dataframe as a CSV.  If a save_path is not provided it will just return a dataframe.
```python
app_user_df = EA.get_app_user_list(app_id=['APP ID'], save_path='C:\\Users\\username\\Documents\\App_User_List.csv')
print(app_user_df.head())

#if no app_id list is added to the function it will return the access list for all apps.
all_apps_user_df = EA.get_app_user_list(save_path='C:\\Users\\username\\Documents\\All_Apps_User_List.csv')
print(all_apps_user_df.head())
```
  
  
To restore a dashboard to a previous version you can use the restore_previous_dashboard_version function and following examples.  The first example will return a dataframe showing the history versions available.  It is generally good to review this file first to view which version you want to restore.  To inspect the JSON of a previous version you can use the second example.  The third example can then be used to revert a dashboard to a previous version.
```python
#View dashboard History
history_df = EA.restore_previous_dashboard_version(dashboard_id='DASHBOARD ID')
history_df.to_csv('C:\\Users\\username\\Documents\\dash_version_history.csv', index=False)

#Get JSON of previous version to review
EA.restore_previous_dashboard_version(dashboard_id='DASHBOARD ID', version_num=1, save_json_path='C:\\Users\\username\\Documents\\jsonFile.json')

#Restore previous version of a dashboard
EA.restore_previous_dashboard_version(dashboard_id='DASHBOARD ID', version_num=1)
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
				"sharedWithId": "USERID"
			}
		]

EA.update_app_access(user_dict=users_to_add, app_id='APPID', update_type='addNewUsers')

#Example of how to remove a user from your app
users_to_remove = [
			{
				"sharedWithId": "USERID"
			}
		]

EA.update_app_access(user_dict=users_to_remove, app_id='APPID', update_type='removeUsers')

#Example of updating access for a user
users_to_update = [
			{
				"accessType": "edit",
				"shareType": "user",
				"sharedWithId": "USERID"
			}
		]

EA.update_app_access(user_dict=users_to_update, app_id='APPID', update_type='updateUsers')
```
