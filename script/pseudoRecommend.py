#coding:utf-8
#!/usr/bin/env python

# エンコーディングの設定
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
import csv
import glob
import json
import subprocess
import logging
from datetime import datetime


## Elastic Search Param
elastichost  = 'elastic_test:9200'
elasticUser  = 'elastic'
elasticPass  = 'changeme'


## Now time
nowTime = datetime.now().strftime("%Y%m%d%H%M%S")

## Directory
homeDir            = '/opt/pseudoRecommend/'
scriptDir          = homeDir + 'script/'
mappingTemplateDir = homeDir + 'template/mapping/'
queryTemplateDir   = homeDir + 'template/query/'
queryFileDir       = homeDir + 'output/' + nowTime + '/queryFile/'
queryResultDir     = homeDir + 'output/' + nowTime + '/queryResult/'
aggResultDir       = homeDir + 'output/' + nowTime + '/aggResult/'
logDir             = homeDir + 'log/'
testDir            = homeDir + 'test/'

os.makedirs(queryFileDir)
os.makedirs(queryResultDir)
os.makedirs(aggResultDir)

## Logging
logging.basicConfig(filename=logDir + 'pseudoRecommend.log', format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)


## Const
replace_str = '#REPLACE_TERGET#'


## mapping定義を読み込み正解データをElastic SearchにIndexする
def indexData(reIndexFlg, mappingFile, correctDataFile):

    logging.info('#### FUNCTION START - indexData')

    ## index名をmappingファイルより作成する。
    indexAlias = os.path.basename(mappingFile.replace('_mapping_template.json', ''))
    indexName  = indexAlias + "." + nowTime

    logging.info('indexAlias=' + indexAlias + ' indexName=' + indexName)

    ## Index作成済みフラグ
    indexMakeFlg = 'no'

    ## 既に同名のAliasが付与されたIndexが存在するかチェックする。
    cmd  = 'curl -s -XGET  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases?pretty"'
    logging.debug('cmd=' + cmd)

    ret, ret_err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    logging.debug('ret=' + json.dumps(ret, sort_keys = True))

    ret_j = json.loads(ret)
    indexList = ret_j.keys()
    indexList.sort()

    cnt = 0
    for index in indexList:
        aliasDic = ret_j[index]
        if (len(aliasDic['aliases']) != 0):
            aliasName = aliasDic['aliases'].keys()

            if ( (indexAlias == aliasName[0])):

                cnt += 1
                ## Index再作成モードの場合は、インデックスを新たに作成しAliasを付け替える。
                if (reIndexFlg == 'yes'):

                    ## 新しいIndexを作成
                    cmd2 = 'curl -s -XPUT  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/' + indexName + '" -d "@' + mappingFile + '"'
                    logging.debug('cmd2=' + cmd2)

                    ret2, ret_err2 = subprocess.Popen(cmd2, shell=True, stdout=subprocess.PIPE).communicate()
                    logging.debug('ret2=' + json.dumps(ret2, sort_keys = True))

                    if (json.loads(ret2).get('acknowledged', False) != True):
                        logging.warning('Failed to create index. indexName=' + indexName)
                        logging.warning('ret2=' + json.dumps(ret2, sort_keys = True))
                        errFlg = 'yes'
                    else:
                        logging.info('Finished to create index. indexName=' + indexName)

                    ## 新しいIndexにAliasを設定
                    cmd3 = 'curl -s -XPOST -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases" -d \'{"actions":[{"add":{"index":"' + indexName + '","alias":"' + indexAlias + '"}}]}\''
                    logging.debug('cmd3=' + cmd3)

                    ret3, ret_err3 = subprocess.Popen(cmd3, shell=True, stdout=subprocess.PIPE).communicate()
                    logging.debug('ret3=' + json.dumps(ret3, sort_keys = True))

                    if (json.loads(ret3).get('acknowledged', False) != True):
                        logging.warning('Failed to add aliase. indexName=' + indexName + ', aliasName=' + indexAlias)
                        logging.warning('ret3=' + json.dumps(ret3, sort_keys = True))
                        errFlg = 'yes'
                    else:
                        logging.info('Finished to add aliase. indexName=' + indexName + ', aliasName=' + indexAlias)

                    ## 古いIndexからAliasを取り外し
                    cmd4 = 'curl -s -XPOST -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases" -d \'{"actions":[{"remove":{"index":"' + index + '","alias":"' + indexAlias + '"}}]}\''
                    logging.debug('cmd4=' + cmd4)

                    ret4, ret_err4 = subprocess.Popen(cmd4, shell=True, stdout=subprocess.PIPE).communicate()
                    logging.debug('ret4=' + json.dumps(ret4, sort_keys = True))

                    if (json.loads(ret4).get('acknowledged', False) != True):
                        logging.warning('Failed to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)
                        logging.warning('ret4=' + json.dumps(ret4, sort_keys = True))
                        errFlg = 'yes'
                    else:
                        logging.info('Finished to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)


                    indexMakeFlg = 'yes'

    ## 同名のAliasが付与されたIndexが存在しない場合はインデックスを新たに作成しAliasを付ける。
    if (cnt == 0):

        ## 新しいIndexを作成
        cmd5 = 'curl -s -XPUT  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/' + indexName + '" -d "@' + mappingFile + '"'
        logging.debug('cmd5=' + cmd5)

        ret5, ret_err5 = subprocess.Popen(cmd5, shell=True, stdout=subprocess.PIPE).communicate()
        logging.debug('ret5=' + json.dumps(ret5, sort_keys = True))

        if (json.loads(ret5).get('acknowledged', False) != True):
            logging.warning('Failed to create index. indexName=' + indexName)
            logging.warning('ret5=' + json.dumps(ret5, sort_keys = True))
            errFlg = 'yes'
        else:
            logging.info('Finished to create index. indexName=' + indexName)

        ## 新しいIndexにAliasを設定
        cmd6 = 'curl -s -XPOST -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases" -d \'{"actions":[{"add":{"index":"' + indexName + '","alias":"' + indexAlias + '"}}]}\''
        logging.debug('cmd6=' + cmd6)

        ret6, ret_err6 = subprocess.Popen(cmd6, shell=True, stdout=subprocess.PIPE).communicate()
        logging.debug('ret6=' + json.dumps(ret6, sort_keys = True))

        if (json.loads(ret6).get('acknowledged', False) != True):
            logging.warning('Failed to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)
            logging.warning('ret6=' + json.dumps(ret6, sort_keys = True))
            errFlg = 'yes'
        else:
            logging.info('Finished to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)

        indexMakeFlg = 'yes'

    ## Indexが作成された場合は正解データをインデックス
    if (indexMakeFlg == 'yes'): 
        cnt2 = 1
        cnt3 = 0
        errCnt = 0

        logging.info('Start to  index correct data. indexName=' + indexName)
        with open( correctDataFile, 'r' ) as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                correctData = '{'
                for i in range(len(header)):

                    ## json syntax errorになるためシングルクウォートを削除
                    row[i] = row[i].replace('\'', '')
                    correctData += '"' + header[i] + '":"' + row[i] + '"'
                    if ( i != len(header)-1 ):
                        correctData += ","
                correctData += '}'

                ## Indexにデータを登録
                cmd7 = 'curl -s -XPUT  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/' + indexName + '/correctData/' + str(cnt2) + '?pretty" -d \'' + correctData + '\''
                logging.debug('cmd7=' + cmd7)

                ret7, ret_err7 = subprocess.Popen(cmd7, shell=True, stdout=subprocess.PIPE).communicate()
                logging.debug('ret7=' + json.dumps(ret7, sort_keys = True))

                if (json.loads(ret7).get('created', False)  != True):
                    logging.warning('Indexed uncorrectly! indexName=' + indexName + ', correctData=' + correctData)
                    logging.warning('ret7=' + json.dumps(ret7, sort_keys = True))
                    errCnt += 1
                    errFlg = 'yes'
                else:
                    logging.debug('Indexed correctly. indexName=' + indexName + ', correctData=' + correctData)
                    cnt3 += 1
                cnt2 +=1
        logging.info('Finished to index correct data to ' + indexName + '. [' + str(cnt3) + '] records were indexed. [' + str(errCnt) + '] records were failed.')

    logging.info('#### FUNCTION END - indexData')
    return ret_j


## Queryの実行
def execQuery2(queryTemplate, mappingFile, correctDataFile):

    logging.info('#### FUNCTION START - execQuery2')

    ## Query実行先のIndexのAlias名
    indexAlias = os.path.basename(mappingFile.replace('_mapping_template.json', ''))

    ##  Queryファイル名をQueryテンプレートファイルより作成する。
    queryFileName = os.path.basename(queryTemplate.replace('_query_template.json', ''))

    logging.info('indexAlias=' + indexAlias + ' queryFileName=' + queryFileName)

    ## 結果をファイルオープン（tsv形式）
    queryResultFile = ''
    queryResultFile = queryResultDir + indexAlias + '_' + queryFileName + '.tsv'
    with open (queryResultFile, 'a') as f:
        writer = csv.writer(f, lineterminator='\n', delimiter='\t')

        ## Queryテンプレートファイル読み込み
        f2=open(queryTemplate, 'r')
        querylines=f2.readlines()
        f2.close()

        ## 正解データとQueryテンプレートよりQueryファイルを生成
        cnt = 0
        err_cnt= 0
        
        with open( correctDataFile, 'rb' ) as f3:
            reader = csv.DictReader(f3, delimiter='\t')
            for row in reader:

                ## 正解データを取得（Query実行時にエラーになるためシングルクォートをエスケープ）
                knowlege_id      = str(row.get('knowlege_id', False).replace('\'', ''))
                knowledge_title  = row.get('knowledge_title', False).replace('\'', '')
                incident_id      = row.get('incident_id', False).replace('\'', '')
                incident_message = row.get('incident_message', False).replace('\'', '')

                ## Queryファイルを作成（json形式）
                queryFile = ''
                queryFile = queryFileDir + queryFileName + '{0:03d}'.format(cnt+1) + '.json'
                f4 = open(queryFile, 'w')
                for line in querylines:
                    rline = line.replace( replace_str, ''.join(incident_message))
                    f4.write( rline )
                f4.close()

                ## Queryの実行
                cmd = 'curl -s -XGET  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/' + indexAlias + '/_search?pretty" -d @'
                cmd = cmd + queryFile
                logging.debug('cmd=' + cmd)

                ret, ret_err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
                logging.debug('ret=' + json.dumps(ret, sort_keys = True))

                ## Queryの実行結果をファイル出力（tsv形式）
                jsonData = json.loads(ret).get('hits', False)
                if (jsonData == False):
                    err_cnt += 1
                    logging.warning('Execute query failed!' + 'queryFile=' + queryFile + 'indexAlias=' + indexAlias + 'result=' + ret)
                else:
                    jsonData = jsonData.get('hits', False)
                    if (jsonData == False):
                        err_cnt += 1
                        logging.warning('Execute query failed!' + 'queryFile=' + queryFile + 'indexAlias=' + indexAlias + 'result=' + ret)
                    else:
                        num                = 0
                        recommend1         = 0
                        recommend3         = 0

                        for jsonDatum in jsonData:
                            r_index            = jsonDatum.get('_index', False)
                            r_type             = jsonDatum.get('_type', False)
                            r_id               = jsonDatum.get('_id', False)
                            r_score            = jsonDatum.get('_score', False)
                            r_source           = jsonDatum.get('_source', False)

                            if (r_source != False):
                                r_knowlege_id      = str(r_source.get('knowlege_id', False))
                                r_knowledge_title  = r_source.get('knowledge_title', False)
                                r_incident_id      = r_source.get('incident_id', False)
                                r_incident_message = r_source.get('incident_message', False) 

                            ## 1位に正解データがレコメンドされるかをチェック
                            if (num == 0):
                                if (r_knowlege_id == knowlege_id):
                                    recommend1 = 1

                            ## 3位以内に正解データがレコメンドされるかをチェック
                            if (num <= 2):
                                if (r_knowlege_id == knowlege_id):
                                    recommend3 = 1

                            ## タイトル行の作成
                            if (cnt == 0):
                                if (num == 0):
                                    title = ['queryFile', 'recommend1', 'recommend3', '_index', '_type', 'knowlege_id', 'knowledge_title', 'incident_id', 'incident_message']
                                title = title + ['_id'+str(num), '_score'+str(num), 'knowledge_id'+str(num), 'knowledge_title'+str(num), 'incident_id'+str(num), 'incident_message'+str(num)]

                            ## 結果行の作成
                            if (num == 0):
                                result_data = [queryFile, recommend1, recommend3, r_index, r_type, knowlege_id, knowledge_title, incident_id, incident_message]
                            result_data = result_data + [r_id, r_score, r_knowlege_id, r_knowledge_title, r_incident_id, r_incident_message]

                            num += 1

                        ## タイトル行の追記
                        if (cnt == 0):
                            writer.writerow(title)

                        ## 結果ファイルへの追記
                        writer.writerow(result_data)

                cnt += 1
    logging.info('Finished to exec query! indexAlias=' + indexAlias + '. queryFileName=' + queryFileName + '. [' + str(cnt) + '] querys were execed. [' + str(err_cnt) + '] querys were failed.')
    logging.info('#### FUNCTION END - execQuery2')
    return cnt


## Queryの実行結果を集計する
def aggResult(queryResultFile):

    logging.info('#### FUNCTION START - aggResult')

    queryResultFileName = queryFileName = os.path.basename(queryResultFile.replace('.tsv', ''))

    cnt = 0
    recommend1_sum = 0
    recommend3_sum = 0

    with open(queryResultFile, 'rb' ) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            recommend1 = row.get('recommend1', False)
            recommend3 = row.get('recommend3', False)

            if (recommend1 != False):
                recommend1_sum = recommend1_sum + int(recommend1)

            if (recommend3 != False):
                recommend3_sum = recommend3_sum + int(recommend3)

            cnt += 1

    ## 1位に正解データがレコメンドされた割合を計算
    recommend1_rate = float(recommend1_sum) / cnt

    ## 3位以内に正解データがレコメンドされた割合を計算
    recommend3_rate = float(recommend3_sum) / cnt

    logging.info('Finished to aggregation! queryResultFileName=' + queryResultFileName + '. ')
    logging.info(str(recommend1_rate) + ' % correct knowledge was recommended as rank 1.')
    logging.info(str(recommend3_rate) + ' % correct knowledge was recommended as rank 3.') 

    makeTitleFlg = 'yes'
    aggResultFile = ''
    aggResultFile = aggResultDir + 'aggQueryResult.tsv'
    if os.path.isfile(aggResultFile):
        makeTitleFlg = 'no'

    with open (aggResultFile, 'a') as f2:
        writer = csv.writer(f2, lineterminator='\n', delimiter='\t')

        ## タイトル行をファイルへ追記
        if (makeTitleFlg == 'yes'):
            title = ['QueryAnalyzePattern', 'recommend1_sum', 'recommend1_rate', 'recommend3_sum', 'recommend3_rate']
            writer.writerow(title)

        ## 集計結果をファイルへ追記
        aggResult = [queryResultFileName, recommend1_sum, recommend1_rate, recommend3_sum, recommend3_rate]
        writer.writerow(aggResult)

    logging.info('#### FUNCTION END - aggResult')
    return cnt


## 集計結果をElastic SearchにIndexする（Kibana表示用）
def indexAggResult():

    logging.info('#### FUNCTION START - indexAggResult')

    ## index名を作成する
    indexAlias = 'pseudoRecommendAggResult'
    indexName  = indexAlias + '.' + nowTime

    logging.info('indexAlias=' + indexAlias + ' indexName=' + indexName)

    ## 既に同名のAliasが付与されたIndexが存在するかチェックする。
    cmd  = 'curl -s -XGET  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases?pretty"'
    logging.debug('cmd=' + cmd)

    ret, ret_err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    logging.debug('ret=' + json.dumps(ret, sort_keys = True))

    ret_j = json.loads(ret)
    indexList = ret_j.keys()
    indexList.sort()

    cnt = 0
    for index in indexList:
        aliasDic = ret_j[index]
        if (len(aliasDic['aliases']) != 0):
            aliasName = aliasDic['aliases'].keys()

            if ( (indexAlias == aliasName[0])):

                cnt += 1

                ## 古いIndexからAliasを取り外し
                cmd2 = 'curl -s -XPOST -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases" -d \'{"actions":[{"remove":{"index":"' + index + '","alias":"' + indexAlias + '"}}]}\''
                logging.debug('cmd2=' + cmd2)

                ret2, ret_err2 = subprocess.Popen(cmd2, shell=True, stdout=subprocess.PIPE).communicate()
                logging.debug('ret2=' + json.dumps(ret2, sort_keys = True))

                if (json.loads(ret2).get('acknowledged', False) != True):
                    logging.warning('Failed to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)
                    logging.warning('ret2=' + json.dumps(ret2, sort_keys = True))
                    errFlg = 'yes'
                else:
                    logging.info('Finished to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)

    ## 新たにIndexを作成しAliasを付ける。
    if (cnt == 0):

        ## 新しいIndexを作成
        cmd3 = 'curl -s -XPUT  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/' + indexName + '"'
        logging.debug('cmd3=' + cmd3)

        ret3, ret_err3 = subprocess.Popen(cmd3, shell=True, stdout=subprocess.PIPE).communicate()
        logging.debug('ret3=' + json.dumps(ret3, sort_keys = True))

        if (json.loads(ret3).get('acknowledged', False) != True):
            logging.warning('Failed to create index. indexName=' + indexName)
            logging.warning('ret3=' + json.dumps(ret3, sort_keys = True))
            errFlg = 'yes'
        else:
            logging.info('Finished to create index. indexName=' + indexName)

        ## 新しいIndexにAliasを設定
        cmd4 = 'curl -s -XPOST -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/_aliases" -d \'{"actions":[{"add":{"index":"' + indexName + '","alias":"' + indexAlias + '"}}]}\''
        logging.debug('cmd4=' + cmd4)

        ret4, ret_err4 = subprocess.Popen(cmd4, shell=True, stdout=subprocess.PIPE).communicate()
        logging.debug('ret4=' + json.dumps(ret4, sort_keys = True))

        if (json.loads(ret4).get('acknowledged', False) != True):
            logging.warning('Failed to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)
            logging.warning('ret6=' + json.dumps(ret4, sort_keys = True))
            errFlg = 'yes'
        else:
            logging.info('Finished to remove aliase. indexName=' + indexName + ', aliasName=' + indexAlias)

    ## Indexが作成された場合は正解データをインデックス
    cnt2 = 1
    cnt3 = 0
    errCnt = 0

    logging.info('Start to  index correct data. indexName=' + indexName)
    aggResultFile = aggResultDir + 'aggQueryResult.tsv'
    with open( aggResultFile, 'r' ) as f:
        reader = csv.reader(f, delimiter='\t')
        header = next(reader)
        for row in reader:
            correctData = '{'
            for i in range(len(header)):

                ## json syntax errorになるためシングルクウォートを削除
                row[i] = row[i].replace('\'', '')
                correctData += '"' + header[i] + '":"' + row[i] + '"'
                if ( i != len(header)-1 ):
                    correctData += ","
            correctData += '}'

            ## Indexにデータを登録
            cmd5 = 'curl -s -XPUT  -u ' + elasticUser + ':' + elasticPass + ' ' + '"http://' + elastichost + '/' + indexName + '/correctData/' + str(cnt2) + '?pretty" -d \'' + correctData + '\''
            logging.debug('cmd5=' + cmd5)

            ret5, ret_err5 = subprocess.Popen(cmd5, shell=True, stdout=subprocess.PIPE).communicate()
            logging.debug('ret5=' + json.dumps(ret5, sort_keys = True))

            if (json.loads(ret5).get('created', False)  != True):
                logging.warning('Indexed uncorrectly! indexName=' + indexName + ', correctData=' + correctData)
                logging.warning('ret5=' + json.dumps(ret5, sort_keys = True))
                errCnt += 1
                errFlg = 'yes'
            else:
                logging.debug('Indexed correctly. indexName=' + indexName + ', correctData=' + correctData)
                cnt3 += 1
            cnt2 +=1
    logging.info('Finished to index correct data to ' + indexName + '. [' + str(cnt3) + '] records were indexed. [' + str(errCnt) + '] records were failed.')

    logging.info('#### FUNCTION END - indexData')
    return ret_j


## main
if __name__ == "__main__":

    if ( len(sys.argv) != 3 ):
        print sys.argv
        print 'Usage: python %s [reIndexFlg] [correctDataFile]' % sys.argv[0]
        quit()
    logging.info('## SCRIPT START - pseudoRecommend.py')

    reIndexFlg      = str(sys.argv[1])
    correctDataFile = str(sys.argv[2]) 

    # for mappingFile in glob.glob(mappingTemplateDir + '*_mapping_template.json'):
    #     result = indexData(reIndexFlg, mappingFile, correctDataFile)

    # for mappingFile in glob.glob(mappingTemplateDir + '*_mapping_template.json'):
    #     for queryTemplate in glob.glob(queryTemplateDir + '*_query_template.json'):
    #          result2 = execQuery2(queryTemplate, mappingFile, correctDataFile)

    for queryResultFile in glob.glob(queryResultDir + '*.tsv'):
        result3 = aggResult(queryResultFile)

    result4 = indexAggResult()

    logging.info('## SCRIPT END - pseudoRecommend.py')
    quit()



