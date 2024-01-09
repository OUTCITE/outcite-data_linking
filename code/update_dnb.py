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

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size      = _configs['chunk_size_dnb'];
_request_timeout = _configs['requestimeout_dnb'];

# WHETHER TO BUFFER THE RESULTING URLS IN A LOCAL DATABASE
_buffer = _configs['buffer_dnb'];

# WHETHER TO CHECK IF THE URL GOES ANYWHERE
_check   = _configs['check_dnb'];
# WETHER TO REDO THE LINKING FOR DOCUMENTS THAT HAVE ALREADY BEEN LABELLED AS PROCESSED FOR THIS STEP BEFORE
_recheck = _configs['recheck_dnb'];
# WHETHER TO TEST THE URL EVEN IF IT WAS ALREADY SEEN BEFORE
_retest  = _configs['retest_dnb']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
# WHETHER TO REPLACE A URL BY THE END OF A REDIRECT CHAIN
_resolve = _configs['resolve_dnb']; # Replaces the URL with the redirected URL if there should be redirection

# REGEX FOR URLS AND DOIS
URL = re.compile(_configs['regex_url']); #r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])'
DOI = re.compile(_configs['regex_doi']); #r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+'
#====================================================================================
# NAME OF THE ELASTICSEARCH INDEX TO MATCH AGAINST
_index_m    = 'dnb';
# FIELD NAME IN THE TARGET INDEX WHICH STORES THE ID FROM MATCHING
_from_field = 'dnb_id';
# WHERE TO ADD THE URL FROM THE ABOVE TARGET INDEX FIELD
_to_field   = 'dnb_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# MAIN FUNCTION TO GET THE URL FOR A REFERENCE IF IT HAS A MATCH ID
def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): #TODO: This not working still, returns no ids
    ids = [];
    for i in range(len(refobjects)):
        url = None;
        ID  = None;
        if id_field in refobjects[i] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            page    = _client_m.search(index=_index_m, query={"term":{"id.keyword":refobjects[i][id_field]}} );
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
        ID = check(url,_resolve,cur,5,USE_BUFFER) if _check else url if url and URL.match(url) else None;
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
            #print(refobjects[i]);
    return set(ids), refobjects;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
_client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENTS INDEX WITH THE URLS
i = 0;
for success, info in bulk(_client,search(_to_field,_from_field,_index,_recheck,get_url,_buffer),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
