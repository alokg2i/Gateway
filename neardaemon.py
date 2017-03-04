# ATTRIBUTE & CONFIGURATION for the sensor 
#type key in attribute would decide whether this is a ZB_module or a Medical_PAD
#State key deides that it's configuration message
#{"attribute": {"type": "ESW", "protocol" "ZB", "version": "11", "model_name": "SZ-ESW02N"}, "thingName": "ZB_ESW_22615"}
#{"state": {"reported": {"powerline": {"active_power": 10, "summation": 20}, "device_state": "off", "reset": false, "singal": {"rssi": 50, "lqi": 20}}}, "clientToken": "ZB_ESW_22615"}
#{"state": {"reported": {"device_state": "on", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}
#{"state": {"reported": {"device_state": "off", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}
import json
import sqlite3
from datetime import datetime
import socket
import sys

def convert_to_dictionary(zMessage):
	print("entering convert to dictionary")
	print(zMessage)
#	print(zMessage['attribute'])
	dpMessage=json.dumps(zMessage)
	print("After dumps %s", dpMessage)
	dcMessage=json.loads(dpMessage)
	print("After loads %s", dcMessage)
	return dcMessage

def getUTCTime():
	utcTime=datetime.utcnow()
	return utcTime


def getThingName(attrDict):
	thingName=attrDict['thingName']
	return thingName
def getEUI64(attrDict):
	if(attrDict.has_key('EUI64')):
		eui64=attrDict['EUI64']
		return eui64
def isModule(attrDict):
	zType=attrDict['type']
	if(zType == "ZB_module"):
		return True
	else:
		return False

def isSensor(attrDict):
	zType=attrDict['type']
	if(zType == "Medical_PAD"):
		return True
	else:
		return False
def isAttribute(msgDict):
	if(msgDict.has_key('attribute')):
		print(' this is attribute message')
		return True
	else:
		print('this is NOT an attribute message!!!')
		print(msgDict)
		return False

def isConfiguration(msgDict):
	if(msgDict.has_key('state')):
		print(' this is a configuration message')
		print(msgDict)
		return True
	else:
		print(' this is NOT a configuration  message')
		return False
def isMedicalMessage(msgDict):
	if(msgDict.has_key('state')):
		currentState=msgDict['state']
		if(currentState.has_key('reported')):
			currentReported=currentState['reported']
			if(currentReported.has_key('medical_pad')):
				return True
			else:
				return False
		else:
			return False
	else:
		return False
def isLeaveMessage(msgDict):
	print("###Inside Leave Message")
	if(msgDict.has_key('state')):
		currentState=msgDict['state']
		if(currentState.has_key('reported')):
			currentReported=currentState['reported']
			if(currentReported.has_key('leave')):
				print("leave is TRUE")
				leavestatus=currentReported['leave']
				print(leavestatus)
				if (leavestatus == True):
					print("Returning true")
					return True
				else:
					print("Returning false")
					return False
			else:
				return False
				
			


#Store EUI64 to thingName Mapping, thingName would keep on changing on each join or 

def storeEUIthingNameMapping(msgDict):
	con = sqlite3.connect("my_first_db.sqlite",60)
	cur = con.cursor()
	thingName=msgDict['thingName']
	attribute=msgDict['attribute']
	EUI64=getEUI64(attribute)
	sensor=isSensor(attribute)
	module=isModule(attribute)
	if(sensor == True):
		cur.execute('''INSERT OR REPLACE INTO ZDEVICEMAP(ThingName, EUI64, IsSensor, IsModule) VALUES(?,?,?,?)''',(thingName,EUI64,int(sensor),int(module)))
	con.commit()
	con.close()
def removeEUI64ThingMapping(eui64):
	con = sqlite3.connect("my_first_db.sqlite",60)
	cur = con.cursor()
#delete from zdevicemap where EUI64='3192981739084526';
 	removeSql='delete from zdevicemap where EUI64="'
        delSql=removeSql+eui64+'"'
        print("delete sql")
        print(delSql)
        cur.execute(delSql)	
	con.commit()
	con.close()
	

def getStoredEUI64(thingName):
	con = sqlite3.connect("my_first_db.sqlite",60)
	cur = con.cursor()
	thingSql='select distinct eui64 from zdevicemap where thingname="'
	finalSql=thingSql+thingName+'"' 
	
	print("######ISSUE WITH SQL######")
	print(finalSql)	
	print("######ISSUE WITH SQL######")	
	cur.execute(finalSql)
	
	#cur.execute("select distinct eui64 from zdevicemap where thingname='Rama'")
	teui64=cur.fetchone() #result is a tuple with single member and tuple is like a list

	print("\n")
	print("######ISSUE######")	
	print(teui64)
	print("######ISSUE######")	
	print("\n")

	print(teui64)
	try:
		eui64=teui64[0]#this should have only one value.
		return eui64
	except:
		print("ERROR" + thingName +"  - the medical pad is NOT paired to the Module")
		return None
	con.close()
	#return eui64
	#from DB
	
	return eui64
def getAttachCentralWetPerimeterWet(msgDict):
	print("Dictionaryi to evaluate attach/wet/peri!!!!")
	print(msgDict)
	currentState=msgDict['state']
	currentReported=currentState['reported']
	medicalPadStatus=currentReported['medical_pad']
	
	print(medicalPadStatus)
	#Only one message at a time, so better check the presence first and then evaluate
	if(medicalPadStatus.has_key('attached')==True):
		if(medicalPadStatus['attached']==True):
			print("####ATTACH TRUE")
			return True,False,False #attach,central_wet,Perimeter_wet
		else:
			return False,False,False
	if(medicalPadStatus.has_key('center_wet')==True):
		if(medicalPadStatus['center_wet']==True):
			print("#### CENTRAL WET TRUE")
			return True,True,False #attach,center_wet,perimeter_wet
		else:
			return True,False,False
	if(medicalPadStatus.has_key('perimeter_wet')==True):
		if(medicalPadStatus['perimeter_wet']==True):
			print("#####PERIMETER TRUE")
			return True,False,True #attach,center_wet,perimeter_wet
		else:
			return True,False,False
	
def storeWetnessInfo(transId,attach,wetness,perimeter,battery,utcTime):
	con = sqlite3.connect("my_first_db.sqlite",60)
	print("attach")
	print(attach)
	print("")
	print("")
	print("wetness")
	print(wetness)
	print("")
	print("")
	print("peerimter")
	print(perimeter)
	print("")
	print("")
	print("EUI64")
	print(transId)
	print("")
	print("")
	
	cur = con.cursor()
	cur.execute('''INSERT INTO INCONTINENCE(trnsMacAddr, IsAttached, IsWet,IsPerimeter, IsBatteryDown, GatewayMac, GatewayIP, utcTime, Status, Sequence, RSSI)
			                        VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(transId,attach,wetness,perimeter,battery,"asdsa","dfdsf",utcTime,4,2,1))
	con.commit()
	con.close()





#temp={"attribute": {"type": "ESW", "protocol": "ZB", "version": "11", "model_name": "SZ-ESW02N","EUI64": "1122334355667888"}, "thingName": "ZB_ESW_22616"}
#temp={"state": {"reported": {"device_state": "off", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}
#temp={"state": {"reported": {"medical_pad":{"attached":"true"},"device_state": "off", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}
#temp={"state": {"reported": {"medical_pad":{"perimeter_wet":"true"},"device_state": "off", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}
#temp={"state": {"reported": {"medical_pad":{"center_wet":"true"},"device_state": "off", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}
#temp={"state": {"reported": {"medical_pad":{"attached":"false"},"device_state": "off", "singal": {"rssi": -23, "lqi": 255}}}, "clientToken": "ZB_ESW_22615"}

sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_address=('localhost',10000)

print('starting up on %s port %s'%server_address)

sock.bind(server_address)

sock.listen(1)

while True:
	print('waiting for connection')
	connection, client_address=sock.accept()
	try:
		while True:
			#Constant buffer of 500 bytes, padded with Zero in the end
			data=connection.recv(500)
			b=isinstance(data,basestring)
			#print("####Is Instance")
			#print(b)
			print("#####data####")
			print(data)
			newdata=data.replace("|","")
			print("")
			print("string below")
			print(newdata)
			print("length below")
			print(len(newdata))
			lastbutindex=newdata.rfind('}')
			lastindex=lastbutindex+1
			#CHECK FOR INDEX
			tempbut=newdata[2:lastindex]
			
			print("before converting to dictionary")
			print(tempbut)
			#print("#### iS a dictionary")
			c=isinstance(tempbut,dict)
			#print(c)
			#print("#### iS a string")
			d=isinstance(tempbut,basestring)
			#print(d)
			msgDict=json.loads(tempbut)
			print("###see this dictionary")
			print(msgDict)
			print("#### iS a dictionary")
			c=isinstance(msgDict,dict)
			print(c)
			#msgDict=convert_to_dictionary(tempbut)
			#msgDict=tempbut
			print("####Convertingto dictionary")
		        print(msgDict)	
			print("########################")

#To Decide whether it's an attribute message or configuration message
#Attribute message should must come before configuration message


			if(isAttribute(msgDict)):
				#Store or update Mapping int the table
				print('before storing\n')
				print("#########Storing should begin")
				storeEUIthingNameMapping(msgDict)
				print("#########Storing should end")
				print('after storing')
	#con = sqlite3.connect("../my_first_db.sqlite")
	#con.row_factory = dict_factory
	#cur = con.cursor()
	#cur.execute('''INSERT OR REPLACE INTO ZDEVICEMAP(ThingName, EUI64, IsSensor, IsModule) VALUES(?,?,?,?)''',("2","3",1,1))
	#con.commit()
	#con.close()
			elif(isConfiguration(msgDict)):
				print("Configuration")
	#Frequent Message
				print(msgDict)

				print("###To Check Whether it is a sensor")

				if (msgDict['clientToken']=="ZB_module"):
					print("### This is a Zigbee Module, Dont get it as you have not set it")
				else:
					print("### This is a Zigbee Sensor, get it as you have set it")
					print("#####ISSUE WITH THING NAME")
#					print(clientToken)
					print(msgDict)
					print("#####ISSUE WITH THING NAME")

					eui64=getStoredEUI64(msgDict['clientToken']) #EUI64 is only available in attribute and is stored in the DB.
					#eui64='3192981739084522'
					print(eui64)
					if(isMedicalMessage(msgDict)):
						attach,wetness,perimeter=getAttachCentralWetPerimeterWet(msgDict)
						utcTime=getUTCTime()
						battery=True;
						if eui64 == None:
							print("ERROR - NOT Sotring this message as Medical Pad is NOT paired")
						
						else:
							storeWetnessInfo(eui64,attach,wetness,perimeter,battery,utcTime)
					elif(isLeaveMessage(msgDict)):
						print("removing EUI64 from the message")
						removeEUI64ThingMapping(eui64)
														
								
					else:
						print("#### message not having medical pad info, that's join info")
			
	#fetch the EUI64 mapping

	#State machine 

	#form the json message
			else:
				print(msgDict)

			break
	finally:
		connection.close()
#msgDict=convert_to_dictionary(temp)

#print("####Convertingto dictionary")
#print(msgDict)
#print("########################")

#To Decide whether it's an attribute message or configuration message
#Attribute message should must come before configuration message


#if(isAttribute(msgDict)):
#	#Store or update Mapping int the table
#	print('before storing')
#	storeEUIthingNameMapping(msgDict)
#	print('after storing')
#	#con = sqlite3.connect("../my_first_db.sqlite")
#	#con.row_factory = dict_factory
#	#cur = con.cursor()
#	#cur.execute('''INSERT OR REPLACE INTO ZDEVICEMAP(ThingName, EUI64, IsSensor, IsModule) VALUES(?,?,?,?)''',("2","3",1,1))
#	#con.commit()
#	#con.close()
#elif(isConfiguration(msgDict)):
#	print("Configuration")
#	#Frequent Message
#	eui64=getStoredEUI64(msgDict['clientToken']) #EUI64 is only available in attribute and is stored in the DB.
#	print(eui64)
#	attach,wetness,perimeter=getAttachCentralWetPerimeterWet(msgDict)
#	utcTime=getUTCTime()
#	battery=True;
#	storeWetnessInfo(eui64,attach,wetness,perimeter,battery,utcTime)


			
	#fetch the EUI64 mapping

	#State machine 

	#form the json message
#else:

#	print(msgDict)


