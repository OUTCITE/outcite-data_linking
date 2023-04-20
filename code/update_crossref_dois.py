#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
from pathlib import Path
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = sys.argv[1]; #'geocite' #'ssoar'

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_buffer = _configs['buffer_crossref'];

_chunk_size      = _configs['chunk_size_crossref'];
_request_timeout = _configs['requestimeout_crossref'];

_recheck = _configs['recheck_crossref'];

_refobjs = _configs['refobjs'];

#====================================================================================
_index_m    = 'crossref'; # Not actually required for crossref as the id is already the doi
_from_field = 'crossref_id';
_to_field   = 'crossref_dois';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): # This actually gets the doi not the url
    ids = [];
    for i in range(len(refobjects)):
        #print(refobjects[i]);
        url = None;
        ID  = None;
        if id_field in refobjects[i]:# and ( _to_field[:-1] not in refobjects[i] or not refobjects[i][_to_field[:-1]] ):
            ID = refobjects[i][id_field];
        else:
            continue;
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
        print(ids);
    return set(ids), refobjects;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
_client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search(_to_field,_from_field,_index,_recheck,get_url,_buffer),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
