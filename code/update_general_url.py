#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
from pathlib import Path
import sqlite3
import re
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index  = sys.argv[1]; #'geocite' #'ssoar'
_target = sys.argv[2] if len(sys.argv)>2 else None;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_buffer = _configs['buffer_general'];

_chunk_size      = _configs['chunk_size_general'];
_request_timeout = _configs['requestimeout_general'];

_check   = _configs['check_general'];
_recheck = _configs['recheck_general'];
_retest  = _configs['retest_general']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
_resolve = _configs['resolve_general']; # Replaces the URL with the redirected URL if there should be redirection

_refobjs = _configs['refobjs'];

ARXIVURL = re.compile(_configs['regex_arxiv_url']); #"((https?:\/\/www\.)|(https?:\/\/)|(www\.))arxiv\.org\/(abs|pdf)\/[0-9]+\.[0-9]+(\.pdf)?"
ARXIVID  = re.compile(_configs['regex_arxiv_id']);  #"[0-9]+\.[0-9]+"

URL = re.compile(_configs['regex_url']); #r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])'
DOI = re.compile(_configs['regex_doi']); #r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+'
#====================================================================================
_from_field = _target+'_id' if _target=='ssoar' or _target=='arxiv' else _target+'_doi' if _target else 'doi';
_to_field   = _target+'_general_urls' if _target else 'extracted_general_urls'; # WARNING: The difference to the usual procedure is that this is used multiple times for different _target, which means processed_general_url=true
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_best_general_url(urls): #TODO: Can be specified
    return urls[0] if len(urls)>0 else None;

def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): # This actually gets the doi not the url
    ids = [];
    for i in range(len(refobjects)):
        #print(id_field,'ssoar_id' in refobjects[i] and refobjects[i]['ssoar_id']);
        url_ = None;
        if id_field=='ssoar_id' and 'ssoar_id' in refobjects[i] and refobjects[i]['ssoar_id'] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            handle  = refobjects[i]['ssoar_id'].split('-')[-1];
            url     = 'https://www.ssoar.info/ssoar/handle/document/'+handle;
            url     = check(url,_resolve,cur,5,USE_BUFFER) if _check else url if url and URL.match(url) else None;
            if url:
                refobjects[i][field[:-1]] = url;
                url_                      = url;
                ids.append(url);
            elif field[:-1] in refobjects[i]:
                refobjects[i][field[:-1]] = None;
                if url in ids: #TODO: Possibly too expensive
                    ids.remove(url);
        elif id_field=='arxiv_id' and 'arxiv_id' in refobjects[i] and refobjects[i]['arxiv_id'] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            url                       = 'https://arxiv.org/abs/'+refobjects[i]['arxiv_id'];
            refobjects[i][field[:-1]] = url;
            url_                      = url;
            ids.append(url);
        elif id_field in refobjects[i] and refobjects[i][id_field] and (_retest or not (_to_field[:-1] in refobjects[i] and refobjects[i][_to_field[:-1]])):
            doi = refobjects[i][id_field].lower().rstrip('.'); print('--->',doi);
            if not ( doi.startswith('arxiv:') or (doi.startswith('abs/') and ARXIVID.search(doi)) ):
                url = doi2url(doi,cur,USE_BUFFER);
                if url and not url.endswith('.pdf') and not ARXIVURL.match(url):
                    refobjects[i][field[:-1]] = url;
                    url_                      = url;
                    ids.append(url);
        #if isinstance(refobjects[i]['general_urls'],str): #TODO: Delete
        #    refobjects[i]['general_urls'] = [];
        if url_:
            refobjects[i]['general_urls'] = list(set(refobjects[i]['general_urls']+[url_])) if 'general_urls' in refobjects[i] else [url_];
            refobjects[i]['general_url']  = get_best_general_url(refobjects[i]['general_urls']);
            print('#####',url_,refobjects[i]['general_urls'])
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
