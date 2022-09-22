from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import requests
import time
import sys

_max_extract_time = 10; #minutes
_max_scroll_tries = 2;
_scroll_size      = 100;

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'cermine_references_from_cermine_refstrings',          #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml' ];

_ids = None;#["EiGe_1976_0001"];

def check(url,RESOLVE=False,timeout=20):
    print('Checking URL',url,'...');
    page   = None;
    status = None;
    try:
        page   = requests.get(url,timeout=timeout);
        status = page.status_code;
        if status == 404:
            print('----> Could not resolve URL due to 404',url);
            return None;
    except Exception as e:
        print('ERROR:',e, file=sys.stderr);
        print('----> Could not resolve URL due to above exception',url);
        return None;
    # page cannot be None unless exception occured
    new_url = page.url;
    if new_url:
        print('Successfully resolved URL',url,'to',new_url);
    else:
        # page.url must be None
        print('----> Could not resolve URL for some reason',url,'-- status:',status);
    return new_url if RESOLVE else url;

def doi2url(doi):
    url   = 'https://doi.org/'+doi;
    return check(url,True);

def doi2url_(doi):
    doi   = 'https://doi.org/'+doi;
    url   = None;
    tries = 0;
    while True:
        try:
            print('Checking DOI',doi,'...');
            url = requests.get(doi,timeout=20).url;
            print('Done checking.');
            print(doi,url);
            break;
        except Exception as e:
            print(e, file=sys.stderr);
            if tries > _max_scroll_tries:
                print(e);
                print('Problem obtaining URL for doi '+doi+'. Giving up.');
                break;
            tries += 1;
            print(e);
            print('Problem obtaining URL for doi '+doi+'. Retrying...');
    return url;

def search(field,id_field,index,recheck,get_url):
    #----------------------------------------------------------------------------------------------------------------------------------
    body      = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+field: True, field: None } } };
    scr_query = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':{'term':{'has_'+field: True}}}} if not recheck else {'bool':{'must':{'term':{'has_'+id_field+'s': True}}}};
    #print(scr_body);
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #print('---------------------------------------------------------------------------------------------\n',doc['_id'],'---------------------------------------------------------------------------------------------\n');
            body        = copy(body);
            body['_id'] = doc['_id'];
            ids         = set(doc['_source'][field]) if field in doc['_source'] and doc['_source'][field] != None else set([]);
            for refobj in _refobjs:
                previous_refobjects            = doc['_source'][refobj] if refobj in doc['_source'] and doc['_source'][refobj] else None;
                new_ids, new_refobjects        = get_url(previous_refobjects,field,id_field) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects);
                ids                           |= new_ids;
                body['_source']['doc'][refobj] = new_refobjects; # The updated ones
                #print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            #print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]        = list(ids) if len(ids) > 0 else None;
            body['_source']['doc']['has_'+field] = True      if len(ids) > 0 else False;
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e, file=sys.stderr);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);
