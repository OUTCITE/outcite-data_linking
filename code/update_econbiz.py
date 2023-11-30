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
_chunk_size      = _configs['chunk_size_econbiz'];
_request_timeout = _configs['requestimeout_econbiz'];

# WHETHER TO BUFFER THE RESULTING URLS IN A LOCAL DATABASE
_buffer = _configs['buffer_econbiz'];

# WHETHER TO CHECK IF THE URL GOES ANYWHERE
_check   = _configs['check_econbiz'];
# WETHER TO REDO THE LINKING FOR DOCUMENTS THAT HAVE ALREADY BEEN LABELLED AS PROCESSED FOR THIS STEP BEFORE
_recheck = _configs['recheck_econbiz'];
# WHETHER TO TEST THE URL EVEN IF IT WAS ALREADY SEEN BEFORE
_retest  = _configs['retest_econbiz']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
# WHETHER TO REPLACE A URL BY THE END OF A REDIRECT CHAIN
_resolve = _configs['resolve_econbiz']; # Replaces the URL with the redirected URL if there should be redirection

# REGEX FOR URLS AND DOIS
URL = re.compile(_configs['regex_url']); #r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])'
DOI = re.compile(_configs['regex_doi']); #r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+'
#====================================================================================
# NAME OF THE ELASTICSEARCH INDEX TO MATCH AGAINST
_index_m    = 'econbiz';
# FIELD NAME IN THE TARGET INDEX WHICH STORES THE ID FROM MATCHING
_from_field = 'econbiz_id';
# WHERE TO ADD THE URL FROM THE ABOVE TARGET INDEX FIELD
_to_field   = 'econbiz_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# MAIN FUNCTION TO GET THE URL FOR A REFERENCE IF IT HAS A MATCH ID
def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): # This actually gets the doi not the url
    ids = [];
    for i in range(len(refobjects)):
        url = None;
        ID  = None;
        if id_field in refobjects[i] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            opa_id = refobjects[i][id_field];
            #page   = _client_m.search(index=_index_m, body={"query":{"term":{"id.keyword":opa_id}}} );
            #doi    = page['hits']['hits'][0]['_source']['doi']  if len(page['hits']['hits'])>0 and 'doi'  in page['hits']['hits'][0]['_source'] else None;
            #urls   = page['hits']['hits'][0]['_source']['urls'] if len(page['hits']['hits'])>0 and 'urls' in page['hits']['hits'][0]['_source'] else [];
            #url    = doi2url(doi,cur,USE_BUFFER) if doi else urls[0] if urls else 'https://www.econbiz.de/Record/'+opa_id;
            url    = 'https://www.econbiz.de/Record/'+opa_id; #urls[0] if (not url) and urls else 'https://www.econbiz.de/Record/'+opa_id if not url else url;
        else:
            continue;
        ID = check(url,_resolve,cur,5,USE_BUFFER) if _check else url if url and URL.match(url) else None;
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
        print(ids);
    return set(ids), refobjects;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
#_client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

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
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
