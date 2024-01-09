DOCINDEX=$1

WORKERS_LINK=16
WORKERS_DOIS=16

LOGFILE=logs/${DOCINDEX}.out
ERRFILE=logs/${DOCINDEX}.err

cd /home/outcite/data_linking/

> $LOGFILE
> $ERRFILE

echo "...creates (and may check) URLs in the straightforward target-collection-dependent way."
for target in ssoar gesis_bib research_data dnb sowiport arxiv; do
    echo $target;
    python code/update_${target}.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE;
done

for target in econbiz crossref openalex; do
    echo $target;
    python code/update_${target}_parallel.py $DOCINDEX $WORKERS >>$LOGFILE 2>>$ERRFILE;
done

echo "...checks if for a match, there are dois available in the matched entry of the respective target collection and writes the into the reference object in the OUTCITE SSOAR index."
for target in research_data openalex econbiz; do
    echo $target;
    python code/update_target_dois.py $DOCINDEX $target >>$LOGFILE 2>>$ERRFILE;
done

echo "crossref"
python code/update_crossref_dois.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE

# This looks up DOIs given for extracted references in either the core repository or in the unpaywall repository, which returns URLs.
# If the URLs are determined to correspond to a PDF file, then they are added as fulltext URL, otherwise as general URL.
# If no target is given, this tries to find URLs for DOIs extracted from the reference string itself rather than looked up from the target collection.

echo "...looks up General URLs for DOI"
for target in ssoar arxiv research_data openalex econbiz crossref; do
    echo $target;
    python code/update_general_url.py $DOCINDEX $target >>$LOGFILE 2>>$ERRFILE;
done

python code/update_general_url.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE

echo "...looks up PDF URLs for DOI"
for repository in core unpaywall; do
    echo $repository;
    for target in ssoar arxiv research_data openalex econbiz crossref; do
        echo $target;
        python code/update_pdf_url.py $DOCINDEX resources/${repository}.db $target $WORKERS_DOIS >>$LOGFILE 2>>$ERRFILE;
    done;
done

for repository in core unpaywall; do
    echo $repository pdf;
    python code/update_pdf_url.py $DOCINDEX resources/${repository}.db >>$LOGFILE 2>>$ERRFILE;
done
