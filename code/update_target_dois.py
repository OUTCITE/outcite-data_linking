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

# THE INDEX TO UPDATE THE REFERENCES IN
_index  = sys.argv[1]; #'geocite' #'ssoar'
_target = sys.argv[2];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size      = _configs['chunk_size_'+_target];
_request_timeout = _configs['requestimeout_'+_target];

# WHETHER TO BUFFER THE RESULTING URLS IN A LOCAL DATABASE
_buffer = _configs['buffer_'+_target];

# WETHER TO REDO THE LINKING FOR DOCUMENTS THAT HAVE ALREADY BEEN LABELLED AS PROCESSED FOR THIS STEP BEFORE
_recheck = _configs['recheck_'+_target];

#====================================================================================
# NAME OF THE ELASTICSEARCH INDEX TO MATCH AGAINST
_index_m    = _target;
# FIELD NAME IN THE TARGET INDEX WHICH STORES THE ID FROM MATCHING
_from_field = _target+'_id';
# WHERE TO ADD THE URL FROM THE ABOVE TARGET INDEX FIELD
_to_field   = _target+'_dois';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# MAIN FUNCTION TO GET THE DOI FOR A REFERENCE IF IT HAS A MATCH ID
def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): # This actually gets the doi not the url
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
            ID     = ID.split('doi.org/')[-1] if isinstance(ID,str) else None; #TODO: This may fail in rare cases where doi.org is in the doi itself
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

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE TARGET IS
_client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENTS INDEX WITH THE DOIS
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
