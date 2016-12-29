This is an implementation on Nifi Site to Site rest api , in python. This module will allow you to send any file that you create in python or is visbile to your python code , to Nifi as a flowfile.It will also allow you to receive the file from NiFi.

To test this...

create  a flow in Nifi, with an input port(igve it a name ex.frompyNifi) ---> logAttribute. start the flow.

Now on your machine,

download the pyNifi source

pyNifi has a dependency on the requests module, install using pip install requests

create a new test.py script.

add following code

##############################BEGIN##########################

from pyNifi import pyNifi

nifi = pyNifi("172.26.248.135",8080)
nifi.sendFile('/Users/knarayanan/test.txt','frompyNifi')


###############################END##########################

execute using python test.py

you should see the following


The file /Users/knarayanan/test.txt was successfully sent to fromnifi with checksum match 132941713


Alternately, you can use the send.py to test the functionality as follows

python send.py [filename with path] [input port name]






