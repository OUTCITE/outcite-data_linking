#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
from copy import deepcopy as copy
import urllib.request
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_index            = sys.argv[1];
_field            = sys.argv[2];
_value            = True if sys.argv[3].lower()=='true' else False if sys.argv[3].lower()=='false' else None if sys.argv[3].lower() in ['none','null'] else sys.argv[3]; # TODO: int?
_overwrite        = len(sys.argv)>4 and sys.argv[4]=='overwrite';
_chunk_size       = 1000;
_scroll_size      = 100;
_max_runtime      = 1; #sec
_max_scroll_tries = 2;

_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': {'doc': { _field: _value } },
        }

_scr_body = {'query':{'match_all':  {} }} if _overwrite else {'query': {'bool': { 'must_not': {'exists':  {'field':_field} }}}};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_docs():
    client = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(_max_runtime*_scroll_size)+'m',size=_scroll_size,body=_scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            _id         = doc['_id'];
            body        = copy(_body);
            body['_id'] = _id;
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(_max_runtime*_scroll_size)+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

print('################# Setting field',_field,'of index',_index,'to',_value,['','overwriting previous value'][_overwrite]);

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,get_docs(),chunk_size=_chunk_size):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    if i % _chunk_size == 0:
        print('refreshing...');
        _client.indices.refresh(index=_index);
        print('refreshed...');
print('refreshing...');
_client.indices.refresh(index=_index);
print('refreshed...');
#-------------------------------------------------------------------------------------------------------------------------------------------------
