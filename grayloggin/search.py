import time
import datetime
import requests
import json
import uuid

from requests.auth import HTTPBasicAuth

# a graylog contains Events and Searches
# a search contains Queries
# a Query contains TimeRanges

class TimeRange():
    def __init__(self, from_time, to_time=None):
        if to_time == None:
            self.type = "relative"
            self.from_time = from_time
        else:
            self.type = "absolute"
            self.from_time = from_time
            self.to_time = to_time

    def toDict(self):
        dictionary = { "type" : self.type }
        if self.type == "absolute":
            dictionary["from"] = self.from_time.isoformat()
            dictionary["to"] = self.to_time.isoformat()
        else:
            dictionary["from"] = self.from_time

        return(dictionary)

class Query():
    def __init__(self, query_string, timerange, streams, page):
        # tunables
        self.pagesize = 50

        # basics
        self.id = str(uuid.uuid4())
        self.search_type_id = str(uuid.uuid4())
        self.timerange = timerange
        self.query_string = query_string
        self.offset = self.pagesize * page

        if type(streams) == type({}):
            self.streams = [ streams ]
        else:
            self.streams = streams


    def setPage(self, page):
        self.offset = self.pagesize * page

    def toDict(self):
        dictionary = {}
        dictionary["id"] = self.id
        dictionary["query"] = { "type": "elasticsearch", "query_string": self.query_string }
        dictionary["timerange"] = self.timerange.toDict()
        dictionary["search_types"] = [ { "id": self.search_type_id, "limit": self.pagesize, "offset": self.offset, "type": "messages" } ]

        if self.streams:
            i = 0
            for search in dictionary["search_types"]:
                dictionary["search_types"][i]["streams"] = [ x['id'] for x in self.streams ]
                i += 1

        return(dictionary)


class SearchResult():
    def __init__(self, string, search_id, searchtype_id):
        self.data = json.loads(string)

        # yeah whatever, basic.
        if self.data["execution"]["done"] == True and self.data["execution"]["cancelled"] == False and self.data["execution"]["completed_exceptionally"] == False:
            self.success = True
        else:
            self.success = False

        # extract the messages
        try:
            self.messages = self.data["results"][search_id]["search_types"][searchtype_id]["messages"]
            # extract some details
            self.total_results = self.data["results"][search_id]["search_types"][searchtype_id]["total_results"]

        except KeyError as e:
            pass
            #print("Results don't exist!", self.data)




class GraylogSearch():
    def __init__(self, graylog):
        
        # import from the associated graylog object
        self.request_headers = graylog.request_headers
        self.request_headers["Content-Type"] = "application/json"
        self.request_headers["Accept"] ="application/json"
            
        self.api_uri = graylog.api_uri
        
        self.auth = graylog.auth

        # state
        self.current_page = 0
        self.events = []

    # a wrapper around Requests to handle retries etc.
    def wrappedPOST(self, uri, data):
        backoff = [ x / 4  for x in range(120) ]
        retries_remaining = 10
        sleeptime = 1

        result = requests.post(uri, auth=self.auth, headers=self.request_headers, data=data)
        while not result.ok and retries_remaining > 0:
            print("r{:02d} s{}", retries_remaining, sleeptime)
            time.sleep(sleeptime)
            sleeptime += random.choice(backoff)
            retries_remaining -= 1
            result = requests.post(uri, auth=self.auth, headers=self.request_headers, data=data)

        if not result.ok:
            raise Exception("Grayloggin'/GraylogSearch/wrappedPOST: Ran out of retries while trying to POST to {} ({:02d} s{})".format(uri, retries_remaining, sleeptime))
        else:
            return(result)


    # Search, private
    def _fetchResults(self):
        # build search
        self.query.setPage(self.current_page)
        self.search = { "id": self.id, "queries": [ self.query.toDict() ] }

        # there is a limit in elastic of 10,000 messages
        if (self.current_page + 1) * self.query.pagesize > 10000:
            return(False)
        
        # perform the actual search
        http_result = self.wrappedPOST("{}api/views/search/sync?timeout=60000".format(self.api_uri), data=json.dumps(self.search))

        # check for error conditions
        try:
            results = SearchResult(http_result.text, self.query.id, self.query.search_type_id)
        except ValueError as e:
            print("Search failed somehow. {}".format(e))
            quit("Bailing")

        if results.success == False:
            print("Search failed.  Raw data: ", results.data['results'][self.query.id]['errors'][0]['description'])
            return(False)

        self.event_count = results.total_results
        self.events = [ x["message"] for x in results.messages ]

        # this needs to return false at the end of results.
        if results.total_results < (self.current_page + 1) * self.query.pagesize:
            return(False)

        # if we made it this far, reset the iteration index and increment the page counter
        self.idx = 0
        self.current_page += 1

    # relative time search
    def searchRelative(self, seconds, query_string, streams=False):
        # regen ID
        self.id = str(uuid.uuid4())
        
        # calc seconds offset in 
        tr = TimeRange(seconds)

        # our search object
        self.query = Query(query_string, tr, streams, 0)

        # fetch the first page
        if self._fetchResults() == False:
            print("Grayloggin': Search failed for some reason!")

    # absolute time search
    def searchAbsolute(self, start, end, streams=False):
        self.id = str(uuid.uuid4())
        pass

    # iteration stuff, "for event in GraylogSearch"
    def __iter__(self):
        self.idx = 0
        return(self)

    def __next__(self):
        try:
            event = self.events[self.idx]
            self.idx += 1
            return(event)
        except IndexError:
            result = self._fetchResults()
            if result == False:
                raise StopIteration
            return(self.__next__())

    def __len__(self):
        # must get the first page.
        if self._fetchResults() == False:
            return(0)
        else:
            return(self.event_count)
