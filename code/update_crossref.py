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
_index            = sys.argv[1]; #'geocite' #'ssoar'

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_chunk_size    = _configs['chunk_size_crossref'];
_requestimeout = _configs['requestimeout_crossref'];

_recheck = _configs['recheck_crossref'];
_retest  = _configs['retest_crossref']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
_resolve = _configs['resolve_crossref']; # Replaces the URL with the redirected URL if there should be redirection

#====================================================================================
_index_m    = 'crossref'; # Not actually required for crossref as the id is already the doi
_from_field = 'crossref_id';
_to_field   = 'crossref_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field):
    ids = [];
    for i in range(len(refobjects)):
        url = None;
        ID  = None;
        if id_field in refobjects[i] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            url = doi2url(refobjects[i][id_field]);
        else:
            #print(id_field,'not in reference.');
            continue;
        ID = check(url,_resolve,5) if url else None;
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
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
