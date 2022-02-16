#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1]; #'geocite' #'ssoar'
_chunk_size       = 200;
_max_extract_time = 0.1; #minutes
_max_scroll_tries =   2;
_scroll_size      = 100;
_requestimeout    =  60;

_recheck = False;

_resolve = False;

#====================================================================================
_index_m    = 'sowiport'; # Not actually required for crossref as the id is already the doi
_from_field = 'sowiport_id';
_to_field   = 'sowiport_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field):
    ids = [];
    for i in range(len(refobjects)):
        print(refobjects[i]);
        url = None;
        ID  = None;
        if id_field in refobjects[i]:
            url = "https://search.gesis.org/publication/"+refobjects[i][id_field];
        else:
            #print(id_field,'not in reference.');
            continue;
        #TODO: This should simply give you a URL and some result snippet or so
        ID = check(url,_resolve);
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
            print(refobjects[i]);
    return set(ids), refobjects;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search(_to_field,_from_field,_index,_recheck,get_url,),chunk_size=_chunk_size, request_timeout=_requestimeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
