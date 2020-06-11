#' this is a set of utility functions for Bloomberg OPENFIGI API
#' Required is a text file called 'openfigiapikey.txt' that has the free openfigi api key obtained at https://www.openfigi.com/

import requests
import pandas as pd
import math
import time
import os.path

openfigi_url_map = 'https://api.openfigi.com/v2/mapping'
fnameapi="openfigiapikey.txt"


if os.path.isfile(fnameapi):
    openfigi_apikey = open(fnameapi,"r").readline().rstrip('\n')
else:
    openfigi_apikey=''



def chunkify(lst,n):
    # cut into chunks
    return [ lst[i::n] for i in range(n) ]


def mapsinglebatch(dt,idType='ID_CUSIP_8_CHR'):
    # get figi mapping based on idType: single batch
    # dt must have a column "idValue"; other additional columns that help with identification, e.g. exchCode can be included; but don't include other columns not used
    # see request format=Request Format under https://www.openfigi.com/api
    dtjobs=dt.assign(idType=idType)
    jobs=dtjobs.to_dict(orient='records')
    job_results = map_jobs(jobs)
    out=job_results_handler(jobs, job_results)
    return out


def mapfigi(dtin,idType='ID_CUSIP_8_CHR',sleepseconds=.5,Nperbatch=100,tmpfile='tmpmap.csv',fileout=''):
    # get figi mapping based on idType multiple batch
    # dtin must have column "idValue"
    # get rid of index if needed
    # limit is 250 requests of 100 jobs per miniute; so pausing .25 seconds should be fine
    chunks = chunkify(dtin, math.ceil(len(dtin)/Nperbatch))
    print('N chunks:',len(chunks),';  est min:', len(chunks)*sleepseconds/60)
    outlist=[]
    errorchunks=[]
    for j in range(len(chunks)):
        try:
            outlist.append(mapsinglebatch(chunks[j],idType=idType))
            #pd.concat(outlist).to_csv(tmpfile)
            #print(j, ' saved')
            time.sleep(sleepseconds)
        except:
            print(j,' except')
            time.sleep(sleepseconds)
            try:
                outlist.append(mapsinglebatch(chunks[j],idType=idType))
                #pd.concat(outlist).to_csv(tmpfile)
                #print(j,' saved 2nd try')
            except:
                print(j, ' except 2nd try')
                errorchunks.append(chunks[j])
                time.sleep(sleepseconds*2)
                pass
    dtfigi=pd.concat(outlist)
    if(fileout):
        dtfigi.to_csv(fileout)
    return dtfigi

def querysinglebatch(dt,n=20):
    # search for figi (slower than mapping): single-match
    # input: dt must have no index, with first column as the tickers to be searched
    # n: max number of rows to query at once
    openfigi_url_search = 'https://api.openfigi.com/v2/search'
    openfigi_headers = {'Content-Type': 'text/json'}
    if openfigi_apikey:
        openfigi_headers['X-OPENFIGI-APIKEY'] = openfigi_apikey
    resl=[]
    nrows=min(n,len(dt))
    for i in range(nrows):
        res=requests.post(url=openfigi_url_search, headers=openfigi_headers,json={"query":dt.iloc[i,0]})#,"securityType2":"Corp"
        if (res.ok):
            try: # this step could get blank data
                dttmp=res.json()['data'][0]
                dttmp['search']=dt.iloc[i,0]
                resl.append(dttmp)
            except:
                print('blank on: ', dt.iloc[i,0])
                pass
        else: # error from request rejections
            print('error on: ', dt.iloc[i,0])
            pass
    if (len(resl)>0):
        dtout=pd.DataFrame(resl)
    else:
        raise ValueError("exit")
    return dtout


def queryfigi(dtin,sleepseconds=65,maxNperquery=15,tmpfile='tmpsave.csv',fileout=''):
    # search for figi (slower than mapquery): multi-batch
    # dtin must have column search term in first column
    # get rid of index if needed
    chunks = chunkify(dtin, math.ceil(len(dtin)/maxNperquery))
    print('N chunks:',len(chunks),';  est hrs:', len(chunks)/60)
    outlist=[]
    errorchunks=[]
    for j in range(len(chunks)):
        try:
            outlist.append(querysinglebatch(chunks[j]))
            pd.concat(outlist).to_csv(tmpfile)
            print(j, ' saved')
            time.sleep(sleepseconds)
        except:
            print(j,' except')
            time.sleep(sleepseconds)
            try:
                outlist.append(querysinglebatch(chunks[j]))
                pd.concat(outlist).to_csv(tmpfile)
                print(j,' saved 2nd try')
            except:
                print(j, ' except 2nd try')
                errorchunks.append(chunks[j])
                time.sleep(sleepseconds*2)
                pass
    dtfigi=pd.concat(outlist)
    if fileout:
        dtfigi.to_csv(fileout)
    return dtfigi




def map_jobs(jobs):
    '''
    Send an collection of mapping jobs to the API in order to obtain the
    associated FIGI(s).
    Parameters
    ----------
    jobs : list(dict)
        A list of dicts that conform to the OpenFIGI API request structure. See
        https://www.openfigi.com/api#request-format for more information. Note
        rate-limiting requirements when considering length of `jobs`.
    Returns
    -------
    list(dict)
        One dict per item in `jobs` list that conform to the OpenFIGI API
        response structure.  See https://www.openfigi.com/api#response-fomats
        for more information.
    '''
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    openfigi_headers = {'Content-Type': 'text/json'}
    if openfigi_apikey:
        openfigi_headers['X-OPENFIGI-APIKEY'] = openfigi_apikey
    response = requests.post(url=openfigi_url, headers=openfigi_headers,
                             json=jobs)
    if response.status_code != 200:
        raise Exception('Bad response code {}'.format(str(response.status_code)))
    return response.json()

def job_results_handler(jobs, job_results):
    '''
    Handle the `map_jobs` results.  See `map_jobs` definition for more info.
    Parameters
    ----------
    jobs : list(dict)
        The original list of mapping jobs to perform.
    job_results : list(dict)
        The results of the mapping job.
    Returns
    -------
        full set of data
    '''
    resl=[]
    for job, result in zip(jobs, job_results):
        tmp=pd.DataFrame(result.get('data', []))
        if(len(tmp)==0): # empty result
            tmp=pd.DataFrame([job['idValue']]).rename(columns={0:job['idType']})
        else:
            tmp[job['idType']]=job['idValue']
        resl.append(tmp)
    out=pd.concat(resl,ignore_index=True,sort=False)
    return out
