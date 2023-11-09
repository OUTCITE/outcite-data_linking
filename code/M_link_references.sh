index=$1

codedir_linking=/home/outcite/data_linking/code/
logdir_linking=/home/outcite/data_linking/logs/

doimapfolder=/home/outcite/data_linking/resources/;

mkdir -p $logdir_linking


for target in sowiport crossref dnb openalex ssoar arxiv econbiz gesis_bib research_data; do
    echo linking to ${target}
    python ${codedir_linking}update_${target}.py ${index} >${logdir_linking}${index}_${target}.out  2>${logdir_linking}${index}_${target}.err;
done

for target in research_data openalex econbiz; do
    echo getting dois from matches to ${target};
    python ${codedir_linking}update_target_dois.py ${index} ${target} >${logdir_linking}${index}_dois_${target}.out  2>${logdir_linking}${index}_dois_${target}.err;
done

echo getting dois from matches to crossref;
python ${codedir_linking}update_crossref_dois.py ${index} >${logdir_linking}${index}_dois_crossref.out  2>${logdir_linking}${index}_dois_crossref.err;

for target in ssoar arxiv research_data openalex econbiz crossref; do
    echo getting general url from ${target} doi;
    python ${codedir_linking}update_general_url.py ${index} ${target} >${logdir_linking}${index}_general_url_${target}.out  2>${logdir_linking}${index}_general_url_${target}.err;
    for database in core unpaywall; do
        echo getting pdf url from ${target} doi via ${database};
        python ${codedir_linking}update_pdf_url.py ${index} ${doimapfolder}${database}.db ${target} 16 >${logdir_linking}${index}_pdf_url_${database}_${target}.out 2>${logdir_linking}${index}_pdf_url${database}_${target}.err;
    done;
done

for database in core unpaywall; do
    echo getting pdf url from extacted doi via ${database};
    python ${codedir_linking}update_pdf_url.py ${index} ${doimapfolder}${database}.db >${logdir_linking}${index}_pdf_url_${database}.out 2>${logdir_linking}${index}_pdf_url${database}.err;
done;
echo getting general url from extracted doi;
python ${codedir_linking}update_general_url.py ${index} >${logdir_linking}${index}_general_url.out  2>${logdir_linking}${index}_general_url.err;
