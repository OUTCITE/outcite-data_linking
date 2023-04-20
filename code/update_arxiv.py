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

_buffer = _configs['buffer_arxiv'];

_chunk_size      = _configs['chunk_size_arxiv'];
_request_timeout = _configs['requestimeout_arxiv'];

_check   = _configs['check_arxiv'];
_recheck = _configs['recheck_arxiv'];
_retest  = _configs['retest_arxiv']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
_resolve = _configs['resolve_arxiv']; # Replaces the URL with the redirected URL if there should be redirection

_refobjs = _configs['refobjs'];

URL = re.compile(r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])');
DOI = re.compile(r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+');
#====================================================================================
_index_m    = 'arxiv'; # Not actually required for crossref as the id is already the doi
_from_field = 'arxiv_id';
_to_field   = 'arxiv_urls';
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): # This actually gets the doi not the url
    ids = [];
    for i in range(len(refobjects)):
        url = None;
        ID  = None;
        if id_field in refobjects[i] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            opa_id = refobjects[i][id_field];
            page   = _client_m.search(index=_index_m, body={"query":{"term":{"id.keyword":opa_id}}} );
            doi    = page['hits']['hits'][0]['_source']['doi'] if len(page['hits']['hits'])>0 and 'doi' in page['hits']['hits'][0]['_source'] else None;
            url    = doi2url(doi,cur,USE_BUFFER) if doi else 'https://arxiv.org/abs/'+opa_id;
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
