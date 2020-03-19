#Python wrapper / library for Einstein Analytics API

import os
import sys
import browser_cookie3
import requests
import json
import time
import datetime
from dateutil import tz
import pandas as pd
import numpy as np
import re
from pandas import json_normalize


class salesforceEinsteinAnalytics(object):
	def __init__(self, env_url, browser):
		self.env_url = env_url
		try:
		    if browser == 'chrome':
		        cj = browser_cookie3.chrome(domain_name=env_url[8:]) #remove first 8 characters since browser cookie does not expect "https://"
		        my_cookies = requests.utils.dict_from_cookiejar(cj)
		        self.header = {'Authorization': 'Bearer '+my_cookies['sid'], 'Content-Type': 'application/json'}
		    elif browser == 'firefox':
		        cj = browser_cookie3.firefox(domain_name=env_url[8:])
		        my_cookies = requests.utils.dict_from_cookiejar(cj)
		        self.header = {'Authorization': 'Bearer '+my_cookies['sid'], 'Content-Type': 'application/json'}
		    else:
		        print('Please select a valid browser (chrome or firefox)')
		        sys.exit(1)
		except:
		    print('ERROR: Could not get session ID.  Make sure you are logged into a live Salesforce session (chrome/firefox).')
		    sys.exit(1)


	#set timezone for displayed operation start time
	def get_local_time(self, add_sec=None, timeFORfile=False):
	    curr_time = datetime.datetime.utcnow().replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())
	    if add_sec is not None:
	        return (curr_time + datetime.timedelta(seconds=add_sec)).strftime("%I:%M:%S %p")
	    elif timeFORfile == True:
	        return curr_time.strftime("%m_%d_%Y__%I%p")
	    else:
	        return curr_time.strftime("%I:%M:%S %p")	


	def get_dataset_id(self, dataset_name, search_type='API Name', verbose=False):
		params = {'pageSize': 50, 'sort': 'Mru', 'hasCurrentOnly': 'true', 'q': dataset_name}
		dataset_json = requests.get(self.env_url+'/services/data/v46.0/wave/datasets', headers=self.header, params=params) 
		dataset_df = json_normalize(json.loads(dataset_json.text)['datasets'])

		#check if the user wants to seach by API name or label name
		if search_type == 'UI Label':
			dataset_df = dataset_df[dataset_df['label'] == dataset_name]
		else:
			dataset_df = dataset_df[dataset_df['name'] == dataset_name]

		#show user how many matches that they got.  Might want to use exact API name if getting multiple matches for label search.
		if verbose == True:
			print('Found '+str(dataset_df.shape[0])+' matching datasets.')

		#if dataframe is empty then return not found message or return the dataset ID
		if dataset_df.empty == True:
			print('Dataset not found.  Please check name or API name in Einstein Analytics.')
			sys.exit(1)
		else:
			dsnm = dataset_df['name'].tolist()[0]
			dsid = dataset_df['id'].tolist()[0]
			
			#get dataset version ID
			r = requests.get(self.env_url+'/services/data/v46.0/wave/datasets/'+dsid, headers=self.header)
			dsvid = json.loads(r.text)['currentVersionId']
			
			return dsnm, dsid, dsvid 


	def run_saql_query(self, saql, save_path=None, verbose=False):
		'''
			This function takes a saql query as an argument and returns a dataframe or saves to csv
			The query can be in JSON form or can be in the UI SAQL form
			load statements must have the appropreate spaces: =_load_\"datasetname\";
		'''
		
		if verbose == True:
			start = time.time()
			print('Checking SAQL and Finding Dataset IDs...')
			print('Process started at: '+str(self.get_local_time()))
		
		saql = saql.replace('\"','\\"') #convert UI saql query to JSON format
		
		#create a dictionary with all datasets used in the query
		load_stmt_old = re.findall(r"(= load )(.*?)(;)", saql)
		load_stmt_new = load_stmt_old.copy()
		for ls in range(0,len(load_stmt_new)):
			load_stmt_old[ls] = ''.join(load_stmt_old[ls])

			dsnm, dsid, dsvid = self.get_dataset_id(dataset_name=load_stmt_new[ls][1].replace('\\"',''), verbose=verbose)
			load_stmt_new[ls] = ''.join(load_stmt_new[ls])
			load_stmt_new[ls] = load_stmt_new[ls].replace(dsnm, dsid+'/'+dsvid)

		#update saql with dataset ID and version ID
		for i in range(0,len(load_stmt_new)):
			saql = saql.replace(load_stmt_old[i], load_stmt_new[i])
		saql = saql.replace('\\"','\"')

		if verbose == True:
			print('Running SAQL Query...')

		#run query and return dataframe or save as csv
		payload = {"query":saql}
		r = requests.post(self.env_url+'/services/data/v46.0/wave/query', headers=self.header, data=json.dumps(payload) )
		df = json_normalize(json.loads(r.text)['results']['records'])
		
		
		if save_path is not None:
			if verbose == True:
				print('Saving result to CSV...')
			
			df.to_csv(save_path, index=False)
			
			if verbose == True:
				end = time.time()
				print('Dataframe saved to CSV...')
				print('Completed in '+str(round(end-start,3))+'sec')
			return df

		else:
			if verbose == True:
				end = time.time()
				print('Completed in '+str(round(end-start,3))+'sec')
			return df


	def restore_previous_dashboard_version(self, dashboard_id, version_num=None, save_json_path=None):
		'''
			version number goes backwards 0 = current version 20 is max oldest version.
			Typically best practice to run the function and view the history first before supplying a version number.
		'''
		#get broken dashboard version history
		r = requests.get(self.env_url+'/services/data/v46.0/wave/dashboards/'+dashboard_id+'/histories', headers=self.header)
		history_df = json_normalize(json.loads(r.text)['histories'])
			
		if save_json_path is not None and version_num is not None:
			preview_link = history_df['previewUrl'].tolist()[version_num]
			r_restore = requests.get(self.env_url+preview_link, headers=self.header)
			with open(save_json_path, 'w', encoding='utf-8') as f:
				json.dump(r_restore.json(), f, ensure_ascii=False, indent=4)
		
		elif version_num is not None:
			payload = { "historyId": history_df['id'].tolist()[version_num] }
			fix = requests.put(self.env_url+history_df['revertUrl'].tolist()[version_num], headers=self.header, data=json.dumps(payload))
		
		else:
			return history_df
		


	def get_app_user_list(self, app_id=None, save_path=None, verbose=False, max_request_attempts=3):
		
		if verbose == True:
			start = time.time()
			progress_counter = 0
			print('Getting app user list and access details...')
			print('Process started at: '+str(self.get_local_time()))

		if app_id is None:
			'''ALERT: CURRENTLY GETTING AN ERROR FOR ALL APP REQUEST
				ERROR = OpenSSL.SSL.SysCallError: (-1, 'Unexpected EOF')
				Proposed Solution is to add a try/except block to handle the error
			'''
			attempts = 0
			while attempts < max_request_attempts:
				try:
					r = requests.get(self.env_url+'/services/data/v46.0/wave/folders', headers=self.header)
					response = json.loads(r.text)
					total_size = response['totalSize']
					next_page = response['nextPageUrl']

					app_user_df = pd.DataFrame()
					break
				except:
					attempts += 1
					if verbose == True:
						print("Unexpected error:", sys.exc_info()[0])
						print("Trying again...")

			for app in response['folders']:
				attempts = 0
				while attempts < max_request_attempts:
					try:
						r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app["id"], headers=self.header)
						users = json.loads(r.text)['shares']
						for u in users: 
							app_user_df = app_user_df.append(	{	"AppId": app['id'], 
																	"AppName": app['name'], 
																	"UserId": u['sharedWithId'], 
																	"UserName": u['sharedWithLabel'], 
																	"AccessType": u['accessType'], 
																	"UserType": u['shareType']
																}, ignore_index=True)
						break
					except:
						attempts += 1
						if verbose == True:
							print("Unexpected error:", sys.exc_info()[0])
							print("Trying again...")

			#continue to pull data from next page
			attempts = 0 # reset attempts for additional pages
			while next_page is not None:
				if verbose == True:
					progress_counter += 25
					print('Progress: '+str(round(progress_counter/total_size*100,1))+'%')

				while attempts < max_request_attempts:
					try:
						np = requests.get(self.env_url+next_page, headers=self.header)
						response = json.loads(np.text)
						next_page = response['nextPageUrl']
						break
					except KeyError:
						next_page = None
						print(sys.exc_info()[0])
						break
					except:
						attempts += 1
						if verbose == True:
							print("Unexpected error:", sys.exc_info()[0])
							print("Trying again...")


				while attempts < max_request_attempts:
					try:
						for app in response['folders']:
							r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app["id"], headers=self.header)
							users = json.loads(r.text)['shares']
							for u in users: 
								app_user_df = app_user_df.append(	{	"AppId": app['id'], 
																		"AppName": app['name'], 
																		"UserId": u['sharedWithId'], 
																		"UserName": u['sharedWithLabel'], 
																		"AccessType": u['accessType'], 
																		"UserType": u['shareType']
																	}, ignore_index=True)
						break
					except:
						attempts += 1
						if verbose == True:
							print("Unexpected error:", sys.exc_info()[0])
							print("Trying again...")


		elif app_id is not None:
			if type(app_id) is list or type(app_id) is tuple:
				for app in app_id:
					app_user_df = pd.DataFrame()
					r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app, headers=self.header)
					response = json.loads(r.text)
					for u in response['shares']: 
						app_user_df = app_user_df.append(	{	"AppId": app, 
																"AppName": response['name'], 
																"UserId": u['sharedWithId'], 
																"UserName": u['sharedWithLabel'], 
																"AccessType": u['accessType'], 
																"UserType": u['shareType']
															}, ignore_index=True)
			else:
				print('Please input a list or tuple of app Ids')
				sys.exit(1)

		
		
		if save_path is not None:
			if verbose == True:
				print('Saving result to CSV...')

			app_user_df.to_csv(save_path, index=False)
			
			if verbose == True:
				end = time.time()
				print('Dataframe saved to CSV...')
				print('Completed in '+str(round(end-start,3))+'sec')

			return app_user_df
			
		else: 
			if verbose == True:
				end = time.time()
				print('Completed in '+str(round(end-start,3))+'sec')
			return app_user_df


	def update_app_access(self, user_dict, app_id, update_type, verbose=False):
		'''
			update types include:  addNewUsers, fullReplaceAccess, removeUsers, updateUsers
		'''
		if verbose == True:
			start = time.time()
			progress_counter = 0
			print('Updating App Access...')
			print('Process started at: '+str(self.get_local_time()))
		
		if update_type == 'fullReplaceAccess':
			shares = user_dict

		elif update_type == 'addNewUsers':
			r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app_id, headers=self.header)
			response = json.loads(r.text)
			shares = response['shares']
			
			#remove fields in the JSON that we don't want
			for s in shares:
				try:
					del s['sharedWithLabel']
				except:
					pass
				try:
					del s['imageUrl']
				except:
					pass
			
			shares = shares + user_dict

		elif update_type == 'removeUsers':
			r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app_id, headers=self.header)
			response = json.loads(r.text)
			shares = response['shares']
			
			to_remove = []
			for u in user_dict:
				to_remove.append(u['sharedWithId'])

			for s in shares:
				if s['sharedWithId'] in to_remove:
					shares.remove(s)

			#remove fields in the JSON that we don't want
			for s in shares:
				try:
					del s['sharedWithLabel']
				except:
					pass
				try:
					del s['imageUrl']
				except:
					pass

		elif update_type == 'updateUsers':
			r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app_id, headers=self.header)
			response = json.loads(r.text)
			shares = response['shares']
			
			to_update = []
			for u in user_dict:
				to_update.append(u['sharedWithId'])

			for s in range(0,len(shares)):
				if shares[s]['sharedWithId'] in to_update:
					shares[s] = next(item for item in user_dict if item["sharedWithId"] == shares[s]['sharedWithId'])

			#remove fields in the JSON that we don't want
			for s in shares:
				try:
					del s['sharedWithLabel']
				except:
					pass
				try:
					del s['imageUrl']
				except:
					pass

		else:
			shares = None
			print('Please choose a user update operation.  Options are: addNewUsers, fullReplaceAccess, removeUsers, updateUsers')
			sys.exit(1)
		
		if shares is not None:
			payload = {"shares": shares}
			r = requests.patch(self.env_url+'/services/data/v46.0/wave/folders/'+app_id, headers=self.header, data=json.dumps(payload))


		if verbose == True:
			end = time.time()
			print('User Access Updated')
			print('Completed in '+str(round(end-start,3))+'sec')


	def update_dashboard_access(self, update_df, update_type, verbose=True):
		'''
			Function to make it easier to update access using dashboard names vs finding all apps needed.
			update dataframe should have the following columns:  Dashboard Id, Access Type, and User Id
		'''
		pass


	def load_csv_to_EA(self, df, xmd):
		'''
			API Documentation: https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_ext_data.meta/bi_dev_guide_ext_data/bi_ext_data_configure_upload.htm
			Might work on this but doesn't seem to be demand for this at the moment.  Let me know and I can add it.
		'''
		pass


if __name__ == '__main__':
	
	#Basic Example of the usage of this function
	#EA = salesforceEinsteinAnalytics(env_url='YOUR SALESFORCE ENVIRONMENT URL', browser='chrome') #choose browser you are using.  Only Chrome and Firefox are supported at the moment.
	
	#Example query looks at top dashboard views and number of users for the current month.  This is in the UI SAQL format.  The JSON format will work too.
	saql = '''q = load "pds_scrcrd_piq_adoption";
				q = filter q by date('data_dt_Year', 'data_dt_Month', 'data_dt_Day') in ["current month".."current month"];
				q = group q by ('app_name', 'lens_name', 'base_route');
				q = foreach q generate 'app_name' as 'app_name', 'lens_name' as 'lens_name', 'base_route' as 'base_route', sum('views') as 'sum_views', unique('usr_id') as 'unique_usr_id';
				q = order q by 'sum_views' desc;
				q = limit q 2000;
	'''

	'''
	result = EA.run_saql_query(saql=saql)
	print(result)

	#Example of app access list
	app_user_df = EA.get_app_user_list(app_id=['APP ID'], save_path='C:\\Users\\username\\Documents\\App_User_List.csv')
	print(app_user_df)

	#View dashboard History
	history_df = EA.restore_previous_dashboard_version(dashboard_id='DASHBOARD ID')
	history_df.to_csv('C:\\Users\\username\\Documents\\dash_version_history.csv', index=False)

	#Get JSON of previous version to review
	EA.restore_previous_dashboard_version(dashboard_id='DASHBOARD ID', version_num=1, save_json_path='C:\\Users\\username\\Documents\\jsonFile.json')

	#Restore previous version of a dashboard
	EA.restore_previous_dashboard_version(dashboard_id='DASHBOARD ID', version_num=1)

	#Example of how to add new users to your app.  Dictionary is in the same format if you want to replace the entire access.  just pass fullReplaceAccess but be careful with this one.
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
	'''
