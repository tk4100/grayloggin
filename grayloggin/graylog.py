# contains Graylog(), for general information about the install we're connected to
import datetime
import requests
import json
import uuid

from requests.auth import HTTPBasicAuth



class Graylog():
    def __init__(self, api_uri, username, password):
        
        # magics
        self.version = "0.2a"
        self.request_headers = { "X-Requested-By" : "GrayLoggin v{}".format(self.version) }

        #normalize ZPI endpoing
        if api_uri[0:4] == "http" and api_uri[-1:] == "/":
            self.api_uri = api_uri
        elif api_uri[0:4] != "http":
            self.api_uri = "http://{}".format(api_uri)
        elif api_uri[-1:] != "/":
            self.api_uri = "{}/".format(api_uri)

        # auth obj
        self.auth = HTTPBasicAuth(username, password)

        # get some of the more static data from our instance
        self.getStreams()

    # get all available streams, { "stream_name" : "<stream_id>" }
    def getStreams(self):

        # clear current streams
        self.streams = json.loads(requests.get("{}api/streams/enabled".format(self.api_uri), auth=self.auth).text)['streams']


