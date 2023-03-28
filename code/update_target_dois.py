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
_index  = sys.argv[1]; #'geocite' #'ssoar'
_target = sys.argv[2];

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_buffer = _configs['buffer_'+_target];

_chunk_size      = _configs['chunk_size_'+_target];
_request_timeout = _configs['requestimeout_'+_target];

_recheck = _configs['recheck_'+_target];

_refobjs = _configs['refobjs'];

#====================================================================================
_index_m    = _target; # Not actually required for crossref as the id is already the doi
_from_field = _target+'_id';
_to_field   = _target+'_dois';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field,cur=None): # This actually gets the doi not the url
    ids = [];
    for i in range(len(refobjects)):
        #print(refobjects[i]);
        url = None;
        ID  = None;
        if id_field in refobjects[i]:# and ( _to_field[:-1] not in refobjects[i] or not refobjects[i][_to_field[:-1]] ):
            opa_id = refobjects[i][id_field];
            page   = _client_m.search(index=_index_m, body={"query":{"term":{"id.keyword":opa_id}}} );
            doi    = page['hits']['hits'][0]['_source']['doi'] if len(page['hits']['hits'])>0 and 'doi' in page['hits']['hits'][0]['_source'] else None;
            ID     = doi[0] if isinstance(doi,list) else doi;
            ID     = ID.split('doi.org/')[-1]; #TODO: This may fail in rare cases where doi.org is in the doi itself
            print(opa_id,doi)
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
