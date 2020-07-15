#Python wrapper / library for Einstein Analytics API
import sys
import logging
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
from decimal import Decimal
import base64
import csv
import unicodecsv
from unidecode import unidecode
import math
import pkg_resources

#init logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(format="%(levelname)s: %(message)s")

class salesforceEinsteinAnalytics(object):
	def __init__(self, env_url, browser, suppressVersionWarn=False):
		self.setLogLvl()
		self.env_url = env_url
		self.__version__ = '0.2.3'
		curr_version = pkg_resources.get_distribution("SalesforceEinsteinAnalytics").version
		if curr_version != self.__version__ and suppressVersionWarn==False:
			logging.warning('New version available. Use pip to upgrade.')
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


	def setLogLvl(self, verbose=False):
		if verbose==True:
		    logging.getLogger().setLevel(logging.INFO)
		else:
		    logging.getLogger().setLevel(logging.WARN)


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
		
		self.setLogLvl(verbose=verbose)

		params = {'pageSize': 50, 'sort': 'Mru', 'hasCurrentOnly': 'true', 'q': dataset_name}
		dataset_json = requests.get(self.env_url+'/services/data/v46.0/wave/datasets', headers=self.header, params=params) 
		dataset_df = json_normalize(json.loads(dataset_json.text)['datasets'])

		#check if the user wants to seach by API name or label name
		if search_type == 'UI Label':
			dataset_df = dataset_df[dataset_df['label'] == dataset_name]
		else:
			dataset_df = dataset_df[dataset_df['name'] == dataset_name]

		#show user how many matches that they got.  Might want to use exact API name if getting multiple matches for label search.
		logging.info('Found '+str(dataset_df.shape[0])+' matching datasets.')

		#if dataframe is empty then return not found message or return the dataset ID
		if dataset_df.empty == True:
			logging.warning('Dataset not found.  Please check name or API name in Einstein Analytics.')
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
		
		self.setLogLvl(verbose=verbose)

		start = time.time()
		logging.info('Checking SAQL and Finding Dataset IDs...')
		logging.info('Process started at: '+str(self.get_local_time()))
		
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

		
		logging.info('Running SAQL Query...')

		#run query and return dataframe or save as csv
		payload = {"query":saql}
		r = requests.post(self.env_url+'/services/data/v46.0/wave/query', headers=self.header, data=json.dumps(payload) )
		df = json_normalize(json.loads(r.text)['results']['records'])
		
		
		if save_path is not None:			
			logging.info('Saving result to CSV...')		
			df.to_csv(save_path, index=False)
			end = time.time()
			logging.info('Dataframe saved to CSV...')
			logging.info('Completed in '+str(round(end-start,3))+'sec')
			return df

		else:
			end = time.time()
			logging.info('Completed in '+str(round(end-start,3))+'sec')
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
		
		self.setLogLvl(verbose=verbose)
		
		start = time.time()
		progress_counter = 0
		logging.info('Getting app user list and access details...')
		logging.info('Process started at: '+str(self.get_local_time()))

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
																	"AppName": app['name'], 
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
				logging.info('Progress: '+str(round(progress_counter/total_size*100,1))+'%')

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
																		"AppName": app['name'], 
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
				logging.error('Please input a list or tuple of app Ids')
				sys.exit(1)

		
		
		if save_path is not None:
			logging.info('Saving result to CSV...')
			app_user_df.to_csv(save_path, index=False)
			end = time.time()
			logging.info('Dataframe saved to CSV...')
			logging.info('Completed in '+str(round(end-start,3))+'sec')
			return app_user_df
			
		else: 
			end = time.time()
			logging.info('Completed in '+str(round(end-start,3))+'sec')
			return app_user_df


	def update_app_access(self, user_dict, app_id, update_type, verbose=False):
		'''
			update types include:  addNewUsers, fullReplaceAccess, removeUsers, updateUsers
		'''
		self.setLogLvl(verbose=verbose)

		start = time.time()
		logging.info('Updating App Access...')
		logging.info('Process started at: '+str(self.get_local_time()))
		
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

		end = time.time()
		logging.info('User Access Updated')
		logging.info('Completed in '+str(round(end-start,3))+'sec')


	def update_dashboard_access(self, update_df, update_type, verbose=True):
		'''
			Function to make it easier to update access using dashboard names vs finding all apps needed.
			update dataframe should have the following columns:  Dashboard Id, Access Type, and User Id
		'''
		self.setLogLvl(verbose=verbose)
		pass


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


	def load_df_to_EA(self, df, dataset_api_name, xmd=None, encoding='UTF-8', operation='Overwrite', useNumericDefaults=True, default_measure_val="0.0", 
		default_measure_fmt="0.0#", charset="UTF-8", deliminator=",", lineterminator="\r\n", removeNONascii=True, ascii_columns=None, fillna=True, dataset_label=None, verbose=False):
		'''
			field names will show up exactly as the column names in the supplied dataframe
		'''

		self.setLogLvl(verbose=verbose)
		
		start = time.time()
		logging.info('Loading Data to Einstein Analytics...')
		logging.info('Process started at: '+str(self.get_local_time()))

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
		
		logging.info('Upload Configuration Complete...')
		logging.info('Chunking and Uploading Data Parts...')

		
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
				data_part64 = base64.b64encode(df_part.to_csv(index=False, header=False, quotechar='"',quoting=csv.QUOTE_MINIMAL).encode('UTF-8')).decode()
			
			range_start += rows_in_part
			max_data_part += rows_in_part
			partnum += 1
			logging.info('\rChunk '+str(chunk+1)+' of '+str(math.ceil(df_memory / MAX_FILE_SIZE))+' completed', end='', flush=True)

			payload = {
				"InsightsExternalDataId" : json.loads(r1.text)['id'],
				"PartNumber" : str(partnum),
				"DataFile" : data_part64
			}

			r2 = requests.post(self.env_url+'/services/data/v46.0/sobjects/InsightsExternalDataPart', headers=self.header, data=json.dumps(payload))
		try:
			json.loads(r2.text)['success'] == True
		except: 
			logging.error('\n Datapart Upload Failed', exc_info=True)
			logging.error(r2.text)
			sys.exit(1)
		
		logging.info('\nDatapart Upload Complete...')


		payload = {
					"Action" : "Process"
				}

		r3 = requests.patch(self.env_url+'/services/data/v46.0/sobjects/InsightsExternalData/'+json.loads(r1.text)['id'], headers=self.header, data=json.dumps(payload))
		
		end = time.time()
		logging.info('Data Upload Process Started. Check Progress in Data Monitor.')
		logging.info('Job ID: '+str(json.loads(r1.text)['id']))
		logging.info('Completed in '+str(round(end-start,3))+'sec')


	def addArchivePrefix(self, warnList, prefix='[ARCHIVE] ', verbose=False):
		'''
		Function to add a warning that an asset will soon be archived.  
		The name of the dashboard will have the chosen prefix added.
		
		max label length is 80 chars and is right trimmed if longer possibly erasing the original title

		Adds prefix to existing label so running twice could overwrite original title
		'''

		self.setLogLvl(verbose=verbose)

		for asset in warnList:
			try:
				r = requests.get(self.env_url+'/services/data/v46.0/wave/dashboards/'+asset, headers=self.header)
				currentLabel = json.loads(r.text)['label']
				newLabel = prefix+currentLabel
				payload = {'label': newLabel[0:79]}
				r = requests.patch(self.env_url+'/services/data/v46.0/wave/dashboards/'+asset, headers=self.header, data=json.dumps(payload))
				if json.loads(r.text)['label'] == prefix+currentLabel:
					logging.info('Successfully updated asset name for: '+asset)
			except:
				try:
					r = requests.get(self.env_url+'/services/data/v46.0/wave/lenses/'+asset, headers=self.header)
					currentLabel = json.loads(r.text)['label']
					newLabel = prefix+currentLabel
					payload = {'label': newLabel[0:79]} #max char len for label = 80
					r = requests.patch(self.env_url+'/services/data/v46.0/wave/lenses/'+asset, headers=self.header, data=json.dumps(payload))
					if json.loads(r.text)['label'] == prefix+currentLabel:
						logging.info('Successfully updated asset name for: '+asset)
				except:
					logging.warning(' could not update asset label: '+asset)
		

	def archiveAssets(self, archiveAppId, ToMoveList, verbose=False):
		'''
		ToMoveList can be the Ids for either a dashboard or a lens
		'''
		self.setLogLvl(verbose=verbose)

		payload = {'folder': {'id':archiveAppId} }

		for asset in ToMoveList:
			try:
				r = requests.patch(self.env_url+'/services/data/v46.0/wave/dashboards/'+asset, headers=self.header, data=json.dumps(payload) )
				if json.loads(r.text)['folder']['id'] == archiveAppId: #check to ensure response has new folder id
					logging.info('Successfully archived (type=dashboard): '+asset)
			except:
				# if response does not contain the new folder id then try same command for a lens
				try:
					r = requests.patch(self.env_url+'/services/data/v46.0/wave/lenses/'+asset, headers=self.header, data=json.dumps(payload) )
					if json.loads(r.text)['folder']['id'] == archiveAppId: #check to ensure response has new folder id
						logging.info('Successfully archived (type=lens): '+asset)
				except:
					logging.warning(' could not move asset: '+asset)


	def getDashboardMetaData(self, appId, max_request_attempts=3, verbose=False):
		
		self.setLogLvl(verbose=verbose)

		convertToDateList = ['createdDate','lastModifiedDate','refreshDate']
		progress_counter = 0
		assets_df = pd.DataFrame()

		params = {
				'folderId': appId,
				'pageSize': 200
		}

		attempts = 0
		while attempts < max_request_attempts:
			try:
				r1 = requests.get(EA.env_url+'/services/data/v46.0/wave/dashboards', headers=EA.header, params=params)
				response = json.loads(r1.text)
				app_assets_df = json_normalize(response['dashboards'])
				total_size = response['totalSize']
				next_page = response['nextPageUrl']
				break
			except:
				attempts += 1
				logging.warning("Unexpected error:", sys.exc_info()[0])
				logging.warning("Trying again...")
		for i in convertToDateList:
			app_assets_df[i].fillna('1900-01-01T00:00:00.000Z', inplace=True)
			app_assets_df[i] = app_assets_df[i].apply(lambda x: pd.to_datetime(x))
		assets_df = assets_df.append(app_assets_df, ignore_index=True)

		#continue to pull data from next page if found
		attempts = 0 # reset attempts for additional pages
		while next_page is not None:
			progress_counter += 200
			logging.info('Progress: '+str(round(progress_counter/total_size*100,1))+'%')

			while attempts < max_request_attempts:
				try:
					r1 = requests.get(EA.env_url+next_page, headers=EA.header, params=params)
					app_assets_df = json_normalize(json.loads(r1.text)['dashboards'])
					next_page = json.loads(r1.text)['nextPageUrl']
					break
				except:
					attempts += 1
					logging.warning("Unexpected error:", sys.exc_info()[0])
					logging.warning("Trying again...")
			for i in convertToDateList:
				app_assets_df[i].fillna('1900-01-01T00:00:00.000Z', inplace=True)
				app_assets_df[i] = app_assets_df[i].apply(lambda x: pd.to_datetime(x))
			assets_df = assets_df.append(app_assets_df, ignore_index=True)
		return assets_df


	def getLensMetaData(self, appId, max_request_attempts=3, verbose=False):
		
		self.setLogLvl(verbose=verbose)

		convertToDateList = ['createdDate','lastModifiedDate','refreshDate']
		progress_counter = 0
		assets_df = pd.DataFrame()

		params = {
				'folderId': appId,
				'pageSize': 200
		}
			
		attempts = 0
		while attempts < max_request_attempts:
			try:
				r1 = requests.get(EA.env_url+'/services/data/v46.0/wave/lenses', headers=EA.header, params=params)
				response = json.loads(r1.text)
				app_assets_df = json_normalize(response['lenses'])
				total_size = response['totalSize']
				next_page = response['nextPageUrl']
				break
			except:
				attempts += 1
				logging.warning("Unexpected error:", sys.exc_info()[0])
				logging.warning("Trying again...")
		for i in convertToDateList:
			app_assets_df[i].fillna('1900-01-01T00:00:00.000Z', inplace=True)
			app_assets_df[i] = app_assets_df[i].apply(lambda x: pd.to_datetime(x))
		assets_df = assets_df.append(app_assets_df, ignore_index=True)

		#continue to pull data from next page if found
		attempts = 0 # reset attempts for additional pages
		while next_page is not None:
			progress_counter += 200
			logging.info('Progress: '+str(round(progress_counter/total_size*100,1))+'%')

			while attempts < max_request_attempts:
				try:
					r1 = requests.get(EA.env_url+next_page, headers=EA.header, params=params)
					app_assets_df = json_normalize(json.loads(r1.text)['lenses'])
					next_page = json.loads(r1.text)['nextPageUrl']
					break
				except:
					attempts += 1
					logging.warning("Unexpected error:", sys.exc_info()[0])
					logging.warning("Trying again...")
			for i in convertToDateList:
				app_assets_df[i].fillna('1900-01-01T00:00:00.000Z', inplace=True)
				app_assets_df[i] = app_assets_df[i].apply(lambda x: pd.to_datetime(x))
			assets_df = assets_df.append(app_assets_df, ignore_index=True)
		return assets_df


	def getAllAssetMetaData(self, appIdList, max_request_attempts=3, verbose=False):
		
		self.setLogLvl(verbose=verbose)

		allAssets_df = pd.DataFrame()

		for app in appIdList:			
				dashboards_df = self.getDashboardMetaData(appId=app, max_request_attempts=max_request_attempts, verbose=verbose)
				allAssets_df = allAssets_df.append(dashboards_df, ignore_index=True)				
				lenses_df = self.getLensMetaData(appId=app, max_request_attempts=max_request_attempts, verbose=verbose)
				allAssets_df = allAssets_df.append(lenses_df, ignore_index=True)			
		
		return allAssets_df


if __name__ == '__main__':	
	pass