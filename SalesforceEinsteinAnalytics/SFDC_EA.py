#Python wrapper / library for Einstein Analytics API

#core libraries
import sys
import logging
import json
import time
from dateutil import tz
import re
from decimal import Decimal
import base64
import csv
import math
import pkg_resources

# installed libraries
import browser_cookie3
import requests
import unicodecsv
from unidecode import unidecode
import datetime
import pandas as pd
from pandas import json_normalize
import numpy as np

#init logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(format="%(levelname)s: %(message)s")

class salesforceEinsteinAnalytics(object):
	def __init__(self, env_url, browser, rawcookie=None, cookiefile=None, logLevel='WARN'):
		self.setLogLvl(level=logLevel)
		self.env_url = env_url
		
		#Check if package is current version
		response = requests.get('https://pypi.org/pypi/SalesforceEinsteinAnalytics/json')
		latest_version = response.json()['info']['version']
		curr_version = pkg_resources.get_distribution("SalesforceEinsteinAnalytics").version
		if curr_version != latest_version:
			logging.info('New version available. Use "pip install SalesforceEinsteinAnalytics --upgrade" to upgrade.')
		
		#get browser cookie to use in request header
		if rawcookie != None:
			self.header = {'Authorization': 'Bearer '+rawcookie, 'Content-Type': 'application/json'}
		elif cookiefile != None:
			print('using cookiefile')
			try:
				if browser == 'chrome':
					cj = browser_cookie3.chrome(domain_name=env_url[8:], cookie_file=cookiefile)
					my_cookies = requests.utils.dict_from_cookiejar(cj)
					self.header = {'Authorization': 'Bearer '+my_cookies['sid'], 'Content-Type': 'application/json'}
				elif browser == 'firefox':
					cj = browser_cookie3.firefox(domain_name=env_url[8:], cookie_file=cookiefile)
					my_cookies = requests.utils.dict_from_cookiejar(cj)
					self.header = {'Authorization': 'Bearer '+my_cookies['sid'], 'Content-Type': 'application/json'}
				else:
					logging.error('Please select a valid browser (chrome or firefox)')
					sys.exit(1)
			except:
				logging.error('ERROR: Could not get session ID.  Make sure you are logged into a live Salesforce session (chrome/firefox).')
				sys.exit(1)
		else:	
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
					logging.error('Please select a valid browser (chrome or firefox)')
					sys.exit(1)
			except:
				logging.error('ERROR: Could not get session ID.  Make sure you are logged into a live Salesforce session (chrome/firefox).')
				sys.exit(1)


	def setLogLvl(self, level='WARN'):
		if level == 'DEBUG':
			logging.getLogger().setLevel(logging.DEBUG)
		elif level == 'INFO':
			logging.getLogger().setLevel(logging.INFO)
		elif level == 'WARN':
			logging.getLogger().setLevel(logging.WARN)
		else:
			logging.getLogger().setLevel(logging.ERROR)


	def get_local_time(self, add_sec=None, timeFORfile=False):
		#set timezone for displayed operation start time
		curr_time = datetime.datetime.utcnow().replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())
		if add_sec is not None:
			return (curr_time + datetime.timedelta(seconds=add_sec)).strftime("%I:%M:%S %p")
		elif timeFORfile == True:
			return curr_time.strftime("%m_%d_%Y__%I%p")
		else:
			return curr_time.strftime("%I:%M:%S %p")	


	def get_dataset_id(self, dataset_name, search_type='API Name', verbose=False):

		if search_type=='API Name':
			try:
				params = {'pageSize': 50, 'sort': 'Mru', 'hasCurrentOnly': 'true', 'q': dataset_name}
				dataset_json = requests.get(self.env_url+'/services/data/v54.0/wave/datasets', headers=self.header, params=params) 
				dataset_df = json_normalize(json.loads(dataset_json.text)['datasets'])
			except:
				logging.error('ERROR: dataset not found using API Name search. Change search type to ID. Details in documentation.')
				sys.exit(1)	
		elif search_type=='ID':
			try:
				params = {'pageSize': 50, 'sort': 'Mru', 'hasCurrentOnly': 'true', 'ids': [dataset_name]}
				dataset_json = requests.get(self.env_url+'/services/data/v54.0/wave/datasets', headers=self.header, params=params) 
				dataset_df = json_normalize(json.loads(dataset_json.text)['datasets'])
			except:
				logging.error('ERROR: dataset not found using ID Name search. Change search type and ensure you have access to the dataset.')
				sys.exit(1)
		else:
			logging.error('ERROR: select an available search_type: API Name, ID, or UI Label')
			sys.exit(1)

		#check if the user wants to seach by API name or label name
		try:
			if search_type == 'UI Label':
				dataset_df = dataset_df[dataset_df['label'] == dataset_name]
			elif search_type == 'ID':
				dataset_df = dataset_df[dataset_df['id'] == dataset_name]
			else:
				dataset_df = dataset_df[dataset_df['name'] == dataset_name]
		except:
			logging.error('Dataset search for {} failed to return a result.  Ensure you have access to the dataset and review the Troubleshooting section in the documentation'.format(dataset_name))
			sys.exit(1)

		#show user how many matches that they got.  Might want to use exact API name if getting multiple matches for label search.
		if verbose == True:
			print('Found '+str(dataset_df.shape[0])+' matching datasets.')

		#if dataframe is empty then return not found message or return the dataset ID
		if dataset_df.empty == True:
			logging.error('Dataset search for {} failed to return a result.  Ensure you have access to the dataset and review the Troubleshooting section in the documentation'.format(dataset_name))
			sys.exit(1)
		else:
			dsnm = dataset_df['name'].tolist()[0]
			dsid = dataset_df['id'].tolist()[0]
			
			#get dataset version ID
			r = requests.get(self.env_url+'/services/data/v46.0/wave/datasets/'+dsid, headers=self.header)
			dsvid = json.loads(r.text)['currentVersionId']
			
			return dsnm, dsid, dsvid 


	def run_saql_query(self, saql, dataset_search_type='API Name', search_for_dataset=True, save_path=None, verbose=False):
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
		
		if search_for_dataset == True:
			#create a dictionary with all datasets used in the query
			load_stmt_old = re.findall(r"(= load )(.*?)(;)", saql)
			load_stmt_new = load_stmt_old.copy()
			for ls in range(0,len(load_stmt_new)):
				load_stmt_old[ls] = ''.join(load_stmt_old[ls])

				dsnm, dsid, dsvid = self.get_dataset_id(dataset_name=load_stmt_new[ls][1].replace('\\"',''), search_type=dataset_search_type, verbose=verbose)
				load_stmt_new[ls] = ''.join(load_stmt_new[ls])
				load_stmt_new[ls] = load_stmt_new[ls].replace(dsnm, dsid+'/'+dsvid)	

			#update saql with dataset ID and version ID
			for i in range(0,len(load_stmt_new)):
				saql = saql.replace(load_stmt_old[i], load_stmt_new[i])
		
		saql = saql.replace('\\"','\"')
			
		if verbose == True:
			print('Running SAQL Query...')
			print(saql)

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
			app_user_df = pd.DataFrame()
			attempts = 0
			while attempts < max_request_attempts:
				try:
					r = requests.get(self.env_url+'/services/data/v46.0/wave/folders', headers=self.header)
					response = json.loads(r.text)
					total_size = response['totalSize']
					next_page = response['nextPageUrl']
					break
				except:
					attempts += 1
					logging.warning("Unexpected error:", sys.exc_info()[0])
					logging.warning("Trying again...")

			for app in response['folders']:
				attempts = 0
				while attempts < max_request_attempts:
					try:
						r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app["id"], headers=self.header)
						users = json.loads(r.text)['shares']
						for u in users: 
							app_user_df = app_user_df.append(	{	"AppId": app['id'], 
																	"AppName": app['label'], 
																	"UserId": u['sharedWithId'], 
																	"UserName": u['sharedWithLabel'], 
																	"AccessType": u['accessType'], 
																	"UserType": u['shareType']
																}, ignore_index=True)
						break
					except:
						attempts += 1
						logging.warning("Unexpected error:", sys.exc_info()[0])
						logging.warning("Trying again...")

			#continue to pull data from next page
			attempts = 0 # reset attempts for additional pages
			while next_page is not None:
				progress_counter += 25
				if verbose == True:
					print('Progress: '+str(round(progress_counter/total_size*100,1))+'%', end='', flush=True)

				while attempts < max_request_attempts:
					try:
						np = requests.get(self.env_url+next_page, headers=self.header)
						response = json.loads(np.text)
						next_page = response['nextPageUrl']
						break
					except KeyError:
						next_page = None
						logging.error(sys.exc_info()[0])
						break
					except:
						attempts += 1
						logging.warning("Unexpected error:", sys.exc_info()[0])
						logging.warning("Trying again...")


				while attempts < max_request_attempts:
					try:
						for app in response['folders']:
							r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app["id"], headers=self.header)
							users = json.loads(r.text)['shares']
							for u in users: 
								app_user_df = app_user_df.append(	{	"AppId": app['id'], 
																		"AppName": app['label'], 
																		"UserId": u['sharedWithId'], 
																		"UserName": u['sharedWithLabel'], 
																		"AccessType": u['accessType'], 
																		"UserType": u['shareType']
																	}, ignore_index=True)
						break
					except:
						attempts += 1
						logging.warning("Unexpected error:", sys.exc_info()[0])
						logging.warning("Trying again...")


		elif app_id is not None:
			app_user_df = pd.DataFrame()
			if type(app_id) is list or type(app_id) is tuple:
				for app in app_id:
					r = requests.get(self.env_url+'/services/data/v46.0/wave/folders/'+app, headers=self.header)
					response = json.loads(r.text)
					for u in response['shares']: 
						app_user_df = app_user_df.append(	{	"AppId": app, 
																"AppName": response['label'], 
																"UserId": u['sharedWithId'], 
																"UserName": u['sharedWithLabel'], 
																"AccessType": u['accessType'], 
																"UserType": u['shareType']
															}, ignore_index=True)
			else:
				logging.error('Please input a list or tuple of app Ids')
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
			logging.error('Please choose a user update operation.  Options are: addNewUsers, fullReplaceAccess, removeUsers, updateUsers')
			sys.exit(1)
		
		if shares is not None:
			payload = {"shares": shares}
			r = requests.patch(self.env_url+'/services/data/v46.0/wave/folders/'+app_id, headers=self.header, data=json.dumps(payload))

		if verbose == True:
			end = time.time()
			print('User Access Updated')
			print('Completed in '+str(round(end-start,3))+'sec')


	def remove_non_ascii(self, df, columns=None):
		if columns == None:
			columns = df.columns
		else:
			columns = columns

		for c in columns:
			if df[c].dtype == "O":
				df[c] = df[c].apply(lambda x: unidecode(x).replace("?",""))


	def create_xmd(self, df, dataset_label, useNumericDefaults=True, default_measure_val="0.0", default_measure_fmt="0.0#", charset="UTF-8", deliminator=",", lineterminator="\r\n"):
		dataset_label = dataset_label
		dataset_api_name = dataset_label.replace(" ","_")

		fields = []
		for c in df.columns:
			if df[c].dtype == "datetime64[ns]":
				name = c.replace(" ","_")
				name = name.replace("__","_")
				date = {
					"fullyQualifiedName": name,
					"name": name,
					"type": "Date",
					"label": c,
					"format": "yyyy-MM-dd HH:mm:ss"
				}
				fields.append(date)
			elif np.issubdtype(df[c].dtype, np.number):
				if useNumericDefaults == True:
					precision = 18
					scale = 2
				elif useNumericDefaults == False:
					precision = df[c].astype('str').apply(lambda x: len(x.replace('.', ''))).max()
					scale = -df[c].astype('str').apply(lambda x: Decimal(x).as_tuple().exponent).min()
				name = c.replace(" ","_")
				name = name.replace("__","_")
				measure = {
					"fullyQualifiedName": name,
					"name": name,
					"type": "Numeric",
					"label": c,
					"precision": precision,
					"defaultValue": default_measure_val,
					"scale": scale,
					"format": default_measure_fmt,
					"decimalSeparator": "."
				}
				fields.append(measure)
			else:
				name = c.replace(" ","_")
				name = name.replace("__","_")
				dimension = {
					"fullyQualifiedName": name,
					"name": name,
					"type": "Text",
					"label": c
				}
				fields.append(dimension)

		xmd = {
			"fileFormat": {
							"charsetName": charset,
							"fieldsDelimitedBy": deliminator,
							"linesTerminatedBy": lineterminator
						},
			"objects": [
						{
							"connector": "CSV",
							"fullyQualifiedName": dataset_api_name,
							"label": dataset_label,
							"name": dataset_api_name,
							"fields": fields
						}
					]
				}       
		return str(xmd).replace("'",'"')


	def load_df_to_EA(self, df, dataset_api_name, xmd=None, encoding='UTF-8', operation='Overwrite', useNumericDefaults=True, default_measure_val="0.0", max_request_attempts=3,
		default_measure_fmt="0.0#", charset="UTF-8", deliminator=",", lineterminator="\r\n", removeNONascii=True, ascii_columns=None, fillna=True, dataset_label=None, verbose=False):
		'''
			field names will show up exactly as the column names in the supplied dataframe
			1) For available operations reference: https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_ext_data.meta/bi_dev_guide_ext_data/bi_ext_data_object_externaldata.htm#topic-title
		'''

		if verbose == True:		
			start = time.time()
			print('Loading Data to Einstein Analytics...')
			print('Process started at: '+str(self.get_local_time()))

		dataset_api_name = dataset_api_name.replace(" ","_")

		if fillna == True:
			for c in df.columns:
				if df[c].dtype == "O":
					df[c].fillna('NONE', inplace=True)
				elif np.issubdtype(df[c].dtype, np.number):
					df[c].fillna(0, inplace=True)
				elif df[c].dtype == "datetime64[ns]":
					df[c].fillna(pd.to_datetime('1900-01-01 00:00:00'), inplace=True)


		if ascii_columns is not None:
			self.remove_non_ascii(df, columns=ascii_columns)
		elif removeNONascii == True:
			self.remove_non_ascii(df)


		## TODO ##
		# Add logic to remove "." from column names.  The period character is not allowed for data table column names

		
		# Upload Config Steps
		if xmd is not None:
			xmd64 = base64.urlsafe_b64encode(json.dumps(xmd).encode(encoding)).decode()
		else:
			xmd64 = base64.urlsafe_b64encode(self.create_xmd(df, dataset_api_name, useNumericDefaults=useNumericDefaults, default_measure_val=default_measure_val, 
				default_measure_fmt=default_measure_fmt, charset=charset, deliminator=deliminator, lineterminator=lineterminator).encode(encoding)).decode()


		upload_config = {
						'Format' : 'CSV',
						'EdgemartAlias' : dataset_api_name,
						'Operation' : operation,
						'Action' : 'None',
						'MetadataJson': xmd64
					}


		r1 = requests.post(self.env_url+'/services/data/v46.0/sobjects/InsightsExternalData', headers=self.header, data=json.dumps(upload_config))
		try:
			json.loads(r1.text)['success'] == True
		except: 
			logging.error(' Upload Config Failed', exc_info=True)
			logging.error(r1.text)
			sys.exit(1)
		
		if verbose == True:
			print('Upload Configuration Complete...')
			print('Chunking and Uploading Data Parts...')

		
		MAX_FILE_SIZE = 10 * 1000 * 1000 - 49
		df_memory = sys.getsizeof(df)
		rows_in_part = math.ceil(df.shape[0] / math.ceil(df_memory / MAX_FILE_SIZE))

		partnum = 0
		range_start = 0
		max_data_part = rows_in_part
		for chunk in range(0, math.ceil(df_memory / MAX_FILE_SIZE)):
			df_part = df.iloc[range_start:max_data_part,:]
			if chunk == 0:
				data_part64 = base64.b64encode(df_part.to_csv(index=False, quotechar='"', quoting=csv.QUOTE_MINIMAL).encode('UTF-8')).decode()
			else:
				data_part64 = base64.b64encode(df_part.to_csv(index=False, header=False, quotechar='"', quoting=csv.QUOTE_MINIMAL).encode('UTF-8')).decode()
			
			range_start += rows_in_part
			max_data_part += rows_in_part
			partnum += 1
			if verbose == True:
				print('\rChunk '+str(chunk+1)+' of '+str(math.ceil(df_memory / MAX_FILE_SIZE))+' completed', end='', flush=True)

			payload = {
				"InsightsExternalDataId" : json.loads(r1.text)['id'],
				"PartNumber" : str(partnum),
				"DataFile" : data_part64
			}

			attempts = 0
			while attempts < max_request_attempts:
				try:
					r2 = requests.post(self.env_url+'/services/data/v46.0/sobjects/InsightsExternalDataPart', headers=self.header, data=json.dumps(payload))
					json.loads(r2.text)['success'] == True
					break
				except: 
					attempts += 1
					logging.error('\n Datapart Upload Failed', exc_info=True)
					logging.debug(r2.text)
					
		
		if verbose == True:
			print('\nDatapart Upload Complete...')


		payload = {
					"Action" : "Process"
				}

		attempts = 0
		while attempts < max_request_attempts:
			try:
				r3 = requests.patch(self.env_url+'/services/data/v46.0/sobjects/InsightsExternalData/'+json.loads(r1.text)['id'], headers=self.header, data=json.dumps(payload))
				break
			except TimeoutError as e:
				attempts += 1
				logging.debug(sys.exc_info()[0])
				logging.warning("Connection Timeout Error.  Trying again...")
		
		if verbose == True:
			end = time.time()
			print('Data Upload Process Started. Check Progress in Data Monitor.')
			print('Job ID: '+str(json.loads(r1.text)['id']))
			print('Completed in '+str(round(end-start,3))+'sec')


	def addArchivePrefix(self, warnList, prefix='[ARCHIVE] ', removePrefix=False, verbose=False):
		'''
		Function to add a warning that an asset will soon be archived.  
		The name of the dashboard will have the chosen prefix added.
		
		max label length is 80 chars and is right trimmed if longer possibly erasing the original title

		Adds prefix to existing label so running twice could overwrite original title
		'''

		for a in range(0,len(warnList)):
			try:
				r = requests.get(self.env_url+'/services/data/v46.0/wave/dashboards/'+warnList[a], headers=self.header)
				currentLabel = json.loads(r.text)['label']
				if removePrefix == True:
					if currentLabel[:len(prefix)] == prefix: #adding check to make sure original lable isn't overwritten
						newLabel = currentLabel[len(prefix):]
				else:
					newLabel = prefix+currentLabel
				payload = {'label': newLabel[0:79]}
				r = requests.patch(self.env_url+'/services/data/v46.0/wave/dashboards/'+warnList[a], headers=self.header, data=json.dumps(payload))
				if json.loads(r.text)['label'] == prefix+currentLabel:
					logging.debug('Successfully updated asset name for: '+warnList[a])
				if verbose == True:
						print('Progress: '+str(round(a/len(warnList)*100,1))+'%', end='', flush=True)
			except:
				try:
					r = requests.get(self.env_url+'/services/data/v46.0/wave/lenses/'+warnList[a], headers=self.header)
					currentLabel = json.loads(r.text)['label']
					if removePrefix == True:
						if currentLabel[:len(prefix)] == prefix: #adding check to make sure original lable isn't overwritten
							newLabel = currentLabel[len(prefix):]
					else:
						newLabel = prefix+currentLabel
					payload = {'label': newLabel[0:79]} #max char len for label = 80
					r = requests.patch(self.env_url+'/services/data/v46.0/wave/lenses/'+warnList[a], headers=self.header, data=json.dumps(payload))
					
					#debugging code that should be removed
					if json.loads(r.text)['label'] == prefix+currentLabel:
						logging.debug('Successfully updated asset name for: '+warnList[a])
					##########################################

					if verbose == True:
							print('Progress: '+str(round(a/len(warnList)*100,1))+'%', end='', flush=True)
				except:
					logging.warning(' could not update asset label: '+warnList[a])
		

	def archiveAssets(self, archiveAppId, ToMoveList, verbose=False):
		'''
		I need to change the archiveAssets function to use a dataframe.  Requireing an asset type will prevent the need
		to try dashboards first, reducing the compute time.  It will also prevent issues of archiving a dashboard when you 
		are trying to archive a lens.  Also, I need to create a dataframe output with the results and not just stdout.
		'''

		payload = {'folder': {'id':archiveAppId} }

		for a in range(0,len(ToMoveList)):
			try:
				r = requests.patch(self.env_url+'/services/data/v46.0/wave/dashboards/'+ToMoveList[a], headers=self.header, data=json.dumps(payload) )
				if json.loads(r.text)['folder']['id'] == archiveAppId: #check to ensure response has new folder id
					if verbose == True:
						print('Progress: '+str(round(a/len(ToMoveList)*100,1))+'%', end='', flush=True)	
					logging.debug('Successfully archived (type=dashboard): '+ToMoveList[a])
			except:
				# if response does not contain the new folder id then try same command for a lens
				try:
					r = requests.patch(self.env_url+'/services/data/v46.0/wave/lenses/'+ToMoveList[a], headers=self.header, data=json.dumps(payload) )
					if json.loads(r.text)['folder']['id'] == archiveAppId: #check to ensure response has new folder id
						if verbose == True:
							print('Progress: '+str(round(a/len(ToMoveList)*100,1))+'%', end='', flush=True)
						logging.debug('Successfully archived (type=lens): '+ToMoveList[a])
				except:
					logging.warning(' could not move asset: '+ToMoveList[a])



	def getMetaData(self, appIdList, objectList=['dashboards','lenses','datasets'], max_request_attempts=3, verbose=False):
		
		progress_counter = 0
		assets_df = pd.DataFrame()

		for a in appIdList:
			if verbose == True:
				progress_counter += 1
				print('Progress: '+str(round(progress_counter/len(appIdList)*100,1))+'%', end='', flush=True)
			params = {'pageSize': 50, 'sort': 'Mru', 'hasCurrentOnly': 'true', 'folderId': a}
			
			for obj in objectList:
				attempts = 0
				while attempts < max_request_attempts:
					try:
						r1 = requests.get(self.env_url+'/services/data/v46.0/wave/'+obj, headers=self.header, params=params)
						response = json.loads(r1.text)
						app_assets_df = json_normalize(response[obj])
						total_size = response['totalSize']
						try:
							next_page = json.loads(r1.text)['nextPageUrl']
						except KeyError as e:
							logging.debug(e)
							next_page = None
						break
					except:
						attempts += 1
						logging.warning("Unexpected error:", sys.exc_info()[0])
						logging.warning("Trying again...")
				assets_df = assets_df.append(app_assets_df, ignore_index=True)

				#continue to pull data from next page if found
				attempts = 0 # reset attempts for additional pages
				while next_page is not None:
					while attempts < max_request_attempts:
						try:
							r1 = requests.get(self.env_url+next_page, headers=self.header, params=params)
							app_assets_df = json_normalize(json.loads(r1.text)[obj])
							try:
								next_page = json.loads(r1.text)['nextPageUrl']
							except KeyError as e:
								logging.debug(e)
								next_page = None
							break
						except:
							attempts += 1
							logging.warning("Unexpected error:", sys.exc_info()[0])
							logging.warning("Trying again...")
					assets_df = assets_df.append(app_assets_df, ignore_index=True)
		for i in assets_df.columns[assets_df.columns.str.contains('Date')]:
			assets_df[i].fillna('1900-01-01T00:00:00.000Z', inplace=True)
			assets_df[i] = assets_df[i].apply(lambda x: pd.to_datetime(x))
		return assets_df


	def getAssetCounts(self, appIdList=None, countsToReturn=['dashboards','lenses','datasets'], max_request_attempts=3, verbose=False):

		if appIdList is not None:
			df = self.getMetaData(appIdList=appIdList, objectList=countsToReturn, verbose=verbose)
			df = df.groupby(['folder.id','folder.label','type'], as_index=True).agg({'id':['count']})
			df = df.pivot_table('id', ['folder.id', 'folder.label'], 'type')
			df.columns = df.columns.droplevel()
			df = df.reset_index()

			updateColNames = {
								'dashboard': 'dashboardCount',
								'dataset': 'datasetCount',
								'lens': 'lensCount',
							}

			df.rename(columns=updateColNames, inplace=True)
			df.columns.names = ['index']			

		else:
			progress_counter = 0
			params = {'pageSize': 50}

			# get list of all folders that the user has access to
			r = requests.get(self.env_url+'/services/data/v48.0/wave/folders', headers=self.header, params=params)
			response = json.loads(r.text)
			apps_df = pd.json_normalize(response['folders'])
			total_size = response['totalSize']
			next_page = response['nextPageUrl']

			#continue to pull data from next page if found
			attempts = 0 # reset attempts for additional pages
			while next_page is not None:
				progress_counter += 50
				if verbose == True:
					print('Collecting App List Progress: '+str(round(progress_counter/total_size*100,1))+'%', end='', flush=True)
				while attempts < max_request_attempts:
					try:
						r1 = requests.get(self.env_url+next_page, headers=self.header, params=params)
						np_df = json_normalize(json.loads(r1.text)['folders'])
						try:
							next_page = json.loads(r1.text)['nextPageUrl']
						except KeyError as e:
							logging.debug(e)
							next_page = None
						break
					except:
						attempts += 1
						logging.warning("Unexpected error:", sys.exc_info()[0])
						logging.warning("Trying again...")
				apps_df = apps_df.append(np_df, ignore_index=True)

			if verbose == True:
				print('Getting asset counts for '+len(apps_df['id'].tolist())+' apps.') 
			
			df = self.getMetaData(appIdList=apps_df['id'].tolist(), objectList=countsToReturn, verbose=verbose)
			df = df.groupby(['folder.id','folder.label','type'], as_index=True).agg({'id':['count']})
			df = df.pivot_table('id', ['folder.id', 'folder.label'], 'type')
			df.columns = df.columns.droplevel()
			df = df.reset_index()

			updateColNames = {
								'dashboard': 'dashboardCount',
								'dataset': 'datasetCount',
								'lens': 'lensCount',
							}

			df.rename(columns=updateColNames, inplace=True)
			df.columns.names = ['index']

		return df

	def get_dashboard_dataset_usage(self, appIdList, verbose=False):

		#https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_resources_dependencies_id.htm

		appAssets = self.getMetaData(appIdList=appIdList, objectList=['dashboards'], verbose=verbose)
		ds_to_db = pd.DataFrame()

		for d in appAssets['id']:
			ds = appAssets[appAssets['id'] == d]
			for i in range(0,len(ds['datasets'].tolist()[0])):
				newRow = {
					'App_ID': ds['folder.id'].values,
					'App_Name': ds['folder.label'].values,
					'Dashboard_ID': [d],
					'Dashboard_APIName': ds['name'].values,
					'Dashboard_Name': ds['label'].values,
					'Dataset_ID': [ds['datasets'].tolist()[0][i].get('id')],
					'Dataset_APIName': [ds['datasets'].tolist()[0][i].get('name')],
					'Dataset_Name': [ds['datasets'].tolist()[0][i].get('label')]
				}
				ds_to_db = ds_to_db.append(pd.DataFrame(newRow), ignore_index=True)

		return ds_to_db

	def update_dashboard_access(self, update_df, update_type, verbose=True):
		'''
			Function to make it easier to update access using dashboard names vs finding all apps needed.
			update dataframe should have the following columns:  Dashboard Id, Access Type, and User Id
		'''
		pass

	def check_dataset_field_usage(self, dataset_api_name, field, verbose=True):
		'''
		Work in progress...
		This is a function to check if a particular dimension is being used in a dataset.  
		It will return a dataframe with the dashboards and steps that the field is used in.
		This will identify if changes need to be made before removing or changing a field.
		'''
		pass


if __name__ == '__main__':	
	pass