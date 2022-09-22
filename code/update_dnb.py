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
_retest  = False;
_resolve = True;

#====================================================================================
_index_m    = 'dnb'; # Not actually required for crossref as the id is already the doi
_from_field = 'dnb_id';
_to_field   = 'dnb_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field): #TODO: For some reason the old incorreect dnb_url is not overwritten
    ids = [];
    for i in range(len(refobjects)):
        url = None;
        ID  = None;
        if id_field in refobjects[i] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            page    = _client_m.search(index=_index_m, body={"query":{"term":{"id":refobjects[i][id_field]}}} );
            dnb_ids = page['hits']['hits'][0]['_source']['ids'] if len(page['hits']['hits'])>0 and 'ids' in page['hits']['hits'][0]['_source'] and len(page['hits']['hits'][0]['_source']['ids'])>0 else [];
            dnb_id  = None;
            for el in dnb_ids:
                if el.startswith('(DE-599)DNB'):
                    dnb_id = el;
                    break;
            suffix = dnb_id[11:] if dnb_id and len(dnb_id)>11 else None;
            url    = "https://d-nb.info/"+suffix if suffix else None;
        else:
            #print(id_field,'not in reference.');
            continue;
        ID = check(url,_resolve) if url else None;
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
print('refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
