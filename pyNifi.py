import requests
import json
import os
import struct
import binascii

class pyNifi(object):
    def __init__(self,host,port):
        self.host=host
        self.port=port
        self.input_ports=[]
        self.__lists2sPorts()
        self.crc=binascii.crc32('')
    
    
    def __lists2sPorts(self):
        resp = requests.get("http://{}:{}/nifi-api/site-to-site".format(self.host,self.port))
        s2sresponse = json.loads(resp.content)
        controller = s2sresponse['controller']
        remoteListeningPort = controller['remoteSiteListeningPort']
        for inputPort in controller['inputPorts']:
            inputPort['remoteSiteListeningPort']=remoteListeningPort
            self.input_ports.append(inputPort)
    
    def chunkData(self,filename,attributes):
        data=None
        totalsize = os.path.getsize(filename)
        if(attributes!=None):
            data= struct.pack(">i",int(len(attributes)))
            for k,v in attributes.iteritems():
                data+= struct.pack(">i",int(len(k)))
                data+= k
                data+= struct.pack(">i",int(len(v)))
                value = v
                data+= value
            data+=struct.pack(">q",long(totalsize))
            self.crc=binascii.crc32(data,self.crc)
            yield data
        else:
            data=struct.pack("i",0)
            data+=struct.pack(">q",long(totalsize))
            self.crc=binascii.crc32(data,self.crc)
            yield data

        with open(filename, 'rb') as file:
            while True:
                chunk = file.read(16000)
                self.crc=binascii.crc32(chunk,self.crc)
                if chunk =='':
                    break
                yield chunk
    
    def sendFile(self,filename,portName,attributes=None):
        s2sPortId=None
        for inputPort in self.input_ports:
            if inputPort['name']==portName:
                if(inputPort['state']!='RUNNING'):
                    raise ValueError('The specified input-port {} was found but is not enabled'.format(portName))
                s2sPortId=inputPort['id']
                break;
        if(s2sPortId==None):
            raise ValueError('The specified input-port {} was not found'.format(portName))
        base_url="http://{}:{}/nifi-api/data-transfer/input-ports/{}/transactions".format(self.host,self.port,s2sPortId)
        headers={}
        
        headers['x-nifi-site-to-site-protocol-version']='1'
        response = requests.post(base_url
                            ,headers=headers)
        jresp = json.loads(response.content)
        trxId = jresp['message'].split(':')[-1]
        headers['content-Type']='application/octet-stream'
        response = requests.post("{}/{}/flow-files".format(base_url,trxId)
                            ,stream=True,headers=headers,
                            data=self.chunkData(filename,attributes))
        if(response.status_code==202 or response.status_code==200):
            tr_conf=response.content
            if(str(tr_conf)==str(self.crc)):
                response = requests.delete("{}/{}?responseCode=12".format(base_url,trxId)
                ,headers=headers)
                if(response.status_code==200):
                    print "The file {} was successfully sent to {} with checksum match {}".format(filename,portName,tr_conf)
            else:
                response = requests.delete("{}/{}?responseCode=19".format(base_url,trxId)
                ,headers=headers)
                if(response.status_code==200):
                    print "The file {} was successfully sent received checksum {} did not match {}, trasaction was deleted with BAD_CHECKSUM".format(filename,self.crc,tr_conf)
        else:
            print "There was an error failed sending {} to {}".format(filename,portName)
            tr_error = response.content
            response = requests.delete("{}/{}?responseCode=15".format(base_url,trxId)
            ,headers=headers)
            raise Error(tr_error)
