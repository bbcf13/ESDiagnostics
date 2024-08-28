
ES_URL=$1
username=$2
password=$3

clusterName=`curl -k -s -X GET -u "${username}:${password}" ${ES_URL} | jq -r .cluster_name`
if [[ "${clusterName}" == "" ]]
then
        echo "Some issue in arguments passed. Please check and run again"
        exit
fi

dateStr=`date +%d%m%YT%H%M`
tmp_dir=/tmp/${dateStr}
rm -rf ${tmp_dir}
mkdir -p ${tmp_dir}


curl -k -X GET -u "${username}:${password}" "${ES_URL}/_nodes?format=json&pretty" > ${tmp_dir}/nodes.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_nodes/stats?format=json&pretty" > ${tmp_dir}/nodes_stats.txt

curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cat/health?format=json&pretty" > ${tmp_dir}/cat_health.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cat/nodes?format=json&pretty" > ${tmp_dir}/cat_nodes.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cat/pending_tasks?format=json&pretty" > ${tmp_dir}/cat_pending_tasks.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cat/indices?format=json&pretty&bytes=b" > ${tmp_dir}/cat_indices.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cat/shards?format=json&pretty" > ${tmp_dir}/cat_shards.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cat/allocation?format=json&pretty" > ${tmp_dir}/cat_allocation.txt

curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cluster/settings?format=json&include_defaults&pretty" > ${tmp_dir}/cluster_settings.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cluster/health?format=json&pretty" > ${tmp_dir}/cluster_health.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_cluster/pending_tasks?format=json&pretty" > ${tmp_dir}/cluster_pending_tasks.txt
curl -k -X GET -u "${username}:${password}" "${ES_URL}/_tasks?format=json&pretty" > ${tmp_dir}/tasks.txt

cd /u01/data/es/logs/
touch ${tmp_dir}/es_search_slowlog.json
ls -rt | grep search_slowlog | tail -10 | while read fileName
do
        grep -vF 'source[{\"size\":0}], id[], "}' ${fileName} >>  ${tmp_dir}/es_search_slowlog.json
done

cd /tmp
ZipReportName=PSR_ESDiagDump_${clusterName}_${dateStr}.zip
rm -rf ${ZipReportName}
zip ${ZipReportName} ${tmp_dir}/*

