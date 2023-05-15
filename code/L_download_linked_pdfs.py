#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
import re
from copy import deepcopy as copy
import urllib.request
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from PyPDF2 import PdfFileWriter, PdfFileReader
from pathlib import Path
from hashlib import sha256
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1];
_httport          = sys.argv[2] if len(sys.argv) >= 3 else '8000';

IN       = open(str((Path(__file__).parent / '../code/').resolve())+'/M_configs_lu.json');
_configs = json.load(IN);
IN.close();

_recheck          = _configs['recheck'];
_redownload       = _configs['redownload'];

_pdfdir           = '/home/outcite/refextract/pdfs/LINKED/';
_address          = 'http://svko-outcite.gesis.intra:'+_httport+'/'+'LINKED/#######.pdf';

_chunk_size       = _configs['chunk_size'];
_scroll_size      = _configs['scroll_size'];
_max_extract_time = _configs['max_extract_time'];
_max_scroll_tries = _configs['max_scroll_tries'];
_request_wait     = _configs['request_wait'];
_request_timeout  = 60;

_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': {'doc': { 'linked_pdfs': []}}
        }

_scr_query = {'bool':{'should':[{'term':{'has_grobid_references_from_grobid_xml':True}},{'term':{'has_anystyle_references_from_grobid_refstrings':True}}]}} if _recheck else {'bool':{'must_not':[{'term':{'processed_linked_pdfs': True}}],'should':[{'term':{'has_grobid_references_from_grobid_xml':True}},{'term':{'has_anystyle_references_from_grobid_refstrings':True}}]}};

ARXIVPDF = re.compile("((https?:\/\/www\.)|(https?:\/\/)|(www\.))arxiv\.org\/pdf\/[0-9]+\.[0-9]+(\.pdf)?");

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def download(address,filename,CROP,ARXIV=False):
    if os.path.isfile(filename) and not _redownload:
        print('Concerning pdf at',address,':', filename,'already exists.');
        return True;
    print('Trying to download',address,'to',filename,end='\r');
    success = True;
    try:
        pdf_file, headers = urllib.request.urlretrieve(address,filename)
        PDF               = open(pdf_file); PDF.close();
    except Exception as exception:
        print(exception);
        print('Failed to download',address,'to',filename);
        success = False;
    if ARXIV and success:
        time.sleep(_request_wait);
    if CROP and success:
        try:
            IN  = PdfFileReader(pdf_file, 'rb'); #TODO: For some reason this works even if the file is not pdf but xml or so
            OUT = PdfFileWriter();
            for i in range(1,IN.getNumPages()):
                OUT.addPage(IN.getPage(i));
            PDF =  open(filename, 'wb');
            OUT.write(PDF); PDF.close();
        except:
            print('Failed to cut off first page from',filename);
            success = False;
    if success:
        print('Successfully downloaded',address,'to',filename);
    return success;

def get_pdfs():
    client = ES(['http://localhost:9200'],timeout=60);
    page     = client.search(index=_index,scroll=str(int(1+_scroll_size*_max_extract_time))+'m',size=100,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #-ONE (ARXIV) DOCUMENT------------------------------------------------------------------------------------------------------------------------
            grobid_xml = doc['_source']['xml'] if 'xml' in doc['_source'] and doc['_source']['xml'] else None;
            #TODO: IF YOU WANT TO DO SOMETHING WITH THE XML <=====
            for pipeline in ['grobid_references_from_grobid_xml','anystyle_references_from_grobid_refstrings']:
                print('Downloading fulltext pdfs linked by',pipeline,'to references',doc['_id']);
                fulltext_pdfs = set([]);
                for refobject in doc['_source'][pipeline]:
                #-ONE REFERENCE-----------------------------------------------------------------------------------------
                    if not ('inline_id' in refobject and refobject['inline_id']):
                        continue;
                    fulltext_urls = [fulltext_url for fulltext_url in refobject['fulltext_urls'] if fulltext_url] if 'fulltext_urls' in refobject and isinstance(refobject['fulltext_urls'],list) else [];
                    for fulltext_url in fulltext_urls:
                        #-ONE FULLTEXT PDF----------------------------------------------------------------
                        filename = sha256(fulltext_url.encode('utf-8')).hexdigest();
                        address  = _address.replace('#######',filename);
                        success  = download(fulltext_url,_pdfdir+filename+'.pdf',ARXIVPDF.match(fulltext_url)) if address else False;
                        if success:
                            fulltext_pdfs.add((refobject['inline_id'],address,));
                            #TODO: IF YOU WANT TO DO SOMETHING WITH THE LINKED PDF <=====
                        #---------------------------------------------------------------------------------
                #-------------------------------------------------------------------------------------------------------
            body                                            = copy(_body);
            body['_id']                                     = doc['_id'];
            body['_source']['doc']['linked_pdfs']           = [[inline_id,fulltext_address] for inline_id,fulltext_address in fulltext_pdfs] if fulltext_pdfs else None;
            body['_source']['doc']['has_linked_pdfs']       = len(fulltext_pdfs)>0;
            body['_source']['doc']['num_linked_pdfs']       = len(fulltext_pdfs);
            body['_source']['doc']['processed_linked_pdfs'] = True;
            #-----------------------------------------------------------------------------------------------------------------------------------------------
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(1+_scroll_size*_max_extract_time))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('WARNING: Some problem occured while scrolling. Sleeping for 3s and retrying...');
                returned        = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,get_pdfs(),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
