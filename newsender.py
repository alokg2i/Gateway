import sqlite3
import json
import urllib
from urllib2 import Request, urlopen, URLError
#import urllib.parse
from datetime import datetime
import uuid
import json
import time


def getUTCTime():
    utcTime=datetime.utcnow()
    return utcTime

def get_windows_mac():
    mac_num = hex(uuid.getnode()).replace('0x', '').upper()
    mac = '-'.join(mac_num[i : i + 2] for i in range(0, 11, 2))
    return mac


def getDeviceState(transId):
	if(statedic.has_key(transId)):
		oldstate=statedic.get(transId)
	else:
		oldstate=4 #if not found, then it's detached, first time
		statedic[transId]=oldstate
        print(transId)
        print(oldstate)
        return oldstate


def setDeviceState(transId, state):
    statedic[transId]=state


def decideWetness(transId, perimeter, wetness):
    currentDeviceState=getDeviceState(transId)
    sendwetness=0

    if(currentDeviceState == 2): #Equvivalent to Initial wetness, wetness can be 1 or 4
        if(perimeter == True):
            sendwetness=4
        elif(wetness == True):
            sendwetness=1

    elif(currentDeviceState == 3): #Equivalent to Subsequent Wetness so always 4
        sendwetness=4
    elif(currentDeviceState == 4): #Detached State, So no Wetness, 2 is timepass
        sendwetness=2
    elif(currentDeviceState == 5):  #Attached State so no wetness, 2 is timepass
        sendwetness=2
    return sendwetness


def decideState(transId, attach, perimeter, wetness):
    print("decidestate entering!!")
    oldDeviceState=getDeviceState(transId)
    newDeviceState=0
    if(oldDeviceState == 4): #Detached State
        if(attach == True):
            newDeviceState=5
        else:
            newDeviceState=oldDeviceState
    elif(oldDeviceState  == 5): #Attached State
        if(perimeter == True):
            newDeviceState=2
        elif(wetness == True):
            newDeviceState=2
        elif(attach == False):
            newDeviceState=4
        else:
            newDeviceState=oldDeviceState
    elif(oldDeviceState == 2):
        if(perimeter == True):
            newDeviceState=3
        elif(attach == False):
            newDeviceState=4
        else:
            newDeviceState=oldDeviceState
    elif(oldDeviceState ==3):
        if(attach == False):
            newDeviceState=4
        else:
            newDeviceState=oldDeviceState

    setDeviceState(transId, newDeviceState)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

#RUN in an INFINITE LOOP
#Have a transaction oriented query
#TWO approaches are possible
# 1. Send in same running thread
# 2. Form a thread for each set of messages for a given device.

statedic={}
while True:
	time.sleep(5)
	con = sqlite3.connect("my_first_db.sqlite")
	con.row_factory = dict_factory
	cur = con.cursor()
	cur.execute("SELECT Distinct trnsMacAddr FROM INCONTINENCE")
	#print cur.fetchone()
	macList=cur.fetchall()
	print(macList)

	print(getUTCTime())

	msgSql='select * from INCONTINENCE where trnsMacAddr="'
	for mac in macList:
#	print(mac["trnsMacAddr"])
#	print(msgSql+ mac["trnsMacAddr"]+'"')
		finalSql=msgSql+str(mac["trnsMacAddr"])+'"'+ " order by sequence"
		print('\n')
		print(finalSql)
		cur.execute(finalSql)
		devicemsgs=cur.fetchall()
		#con.commit()
		#con.close()
		print devicemsgs
		for msgs in devicemsgs:
			print(msgs)
			print("#####################")
			print(json.dumps(msgs))
			jsonMsg=json.dumps(msgs)
			transId=msgs["trnsMacAddr"]
			print("#############transmid##########################")
			print(transId)
			wetness=msgs['IsWet']
			perimeter=msgs['IsPerimeter']
			attach=msgs['IsAttached']
			decideState(transId,attach,perimeter,wetness)
			diaperWetLevel=decideWetness(transId,perimeter,wetness)
			jsondic={}
       # data='{"eventId":5,"trnsMacAddr":"93","devMacAddr":"93", "ipAddress":"MacIS", "model":"Transmitter","utcTime":"2016-06-28T04:04:11.467","exposure":2,"wetness":0,"proximity":"Savitha123","voltage":2}'
			jsondic['eventId']=getDeviceState(transId)
			jsondic['trnsMacAddr']=str(transId)[-8:]
			jsondic['devMacAddr']=get_windows_mac()
			jsondic['ipAddress'] = str(102)
			utcTime=getUTCTime()
			print(utcTime)
			jsondic['utcTime']=str(utcTime)
			if(diaperWetLevel == 2):
				jsondic['wetness']=""
			else:
				jsondic['wetness'] = diaperWetLevel
			
			jsondic['proximity'] = str(3)

	       #        print(jsondic)
			sendmessage=json.dumps(jsondic)
			print("Before Send messsage")
			print(sendmessage)

			messagesend={'text':jsondic}

			print(messagesend)

		#params=urllib.parse.urlencode({'text': sendmessage})

			params=urllib.urlencode(messagesend)
			print("params#########")
			print("")
			print(params)


			url="http://54.153.61.242:8080/data-receiver/NearMessageServlet"
			#req=urllib2.Request(url,params)
			#req=urllib2.Request(url,params)
			#response=urllib2.urlopen(req)

			req=Request(url,params)
			print("Reqquest######")

			print(req)

			try:
				response=urlopen(req)
			except URLError as e:
				if hasattr(e,'reason'):
					print 'we failed to reach the server'
					print 'Reason: ', e.reason
				elif hasattr(e,'code'):
					print 'The Server could not fulfill the request'
					print 'Error code: ', e.code
			else:
				print 'Message Sent SUCCESSFULY!!'
				removeSql='delete from incontinence where trnsMacAddr="'
				delSql=removeSql+transId+'"'
				print("delete sql")
				print(delSql)
				cur.execute(delSql)
	con.commit()
	con.close()



	print("G2i NEAR")


#		resp=urllib.request.urlopen(url)
		
#		print(resp.status)

#		removeSql='delete from incontinence where trnsMacAddr="'

#		finalSql=msgSql+str(mac["trnsMacAddr"])+'"'+ " order by sequence"
#		delSql=removeSql+transId+'"'
#		print(delSql)
#		if(resp.status==200):
#		cur.execute(delSql)
#			con.commit()





		



#for mac in macList:
#	cur.execute(msgSql+''.join(mac))
#	msg=cur.fetchall()
#	print msg
	


