#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
import requests
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1]; #'geocite' #'ssoar'
_chunk_size       =  50;

_max_scroll_tries =   2;
_scroll_size      =  25;
_requestimeout    =  60;

_recheck = False;
<<<<<<< HEAD
_retest  = False;
_resolve = True;
=======
_retest  = False; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
_resolve = True;  # Replaces the URL with the redirected URL if there should be redirection
>>>>>>> master

#====================================================================================
_index_m    = 'openalex'; # Not actually required for crossref as the id is already the doi
_from_field = 'openalex_id';
_to_field   = 'openalex_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field):
    ids = [];
    for i in range(len(refobjects)):
        #print(refobjects[i]);
        url = None;
        ID  = None;
        if id_field in refobjects[i] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            opa_id = refobjects[i][id_field];
            page   = _client_m.search(index=_index_m, body={"query":{"term":{"id":opa_id}}} );
            doi    = doi2url(page['hits']['hits'][0]['_source']['doi']) if len(page['hits']['hits'])>0 and 'doi' in page['hits']['hits'][0]['_source'] else None;
            link   = page['hits']['hits'][0]['_source']['url'] if len(page['hits']['hits'])>0 and 'doi' in page['hits']['hits'][0]['_source'] else None;
            url    = doi if doi else link if link else opa_id if opa_id else None;
            print(url);
        else:
            #print(id_field,'not in reference.');
            continue;
        ID = check(url,_resolve,5);
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
            #print(refobjects[i]);
    return set(ids), refobjects;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
_client_m = ES(['localhost'],scheme='http',port=9200,timeout=60);

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
