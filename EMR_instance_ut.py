from datetime import datetime, timedelta
import boto3
import time
import csv,sys
import random
import pprint

client = boto3.client('emr',region_name="us-east-1")
cwclient = boto3.client('cloudwatch',region_name="us-east-1")
pp = pprint.PrettyPrinter(depth=2)

def getClusterIds(starttime, endtime):
    clusterids = []
    wehavemore = True
    response = client.list_clusters(
        CreatedAfter=starttime,
        CreatedBefore=endtime,
        ClusterStates=[
            'TERMINATED'
        ],
    )
    while wehavemore:
        print(response['Clusters'])
        for clusObj in response['Clusters']:
            clusterid = clusObj['Id']
            clusterids.append(clusterid)
        if 'Marker' in response:
            marker = response['Marker']
        else:
            wehavemore = False
            break
        response = client.list_clusters(
            CreatedAfter=starttime,
            CreatedBefore=endtime,
            ClusterStates=[
                'TERMINATED'
            ],
            Marker=marker
        )
        print("Clusters ids fetched for - "+str(len(clusterids)) +" clusters, fetching more")
    return clusterids

def getCpuUtilization(clusterid):
    intancesresp = client.list_instances(
        ClusterId=clusterid
    )
    totalInstances = len(intancesresp['Instances'])
    instanceToInspect=[]
    for i in range(20):
        if totalInstances<20:
            if i <= (totalInstances -1):
                instanceToInspect.append(i)
        else:
            instanceToInspect.append(random.randrange(0, totalInstances-1, 1))
    uniqueindexes=set(instanceToInspect)
    averages=[]
    for i in uniqueindexes:
        # print(i,intancesresp['Instances'][i])
        instanceId=intancesresp['Instances'][i]['Ec2InstanceId']
        period=600
        if 'ReadyDateTime' in intancesresp['Instances'][i]['Status']['Timeline']:
            starttime = intancesresp['Instances'][i]['Status']['Timeline']['ReadyDateTime'] + timedelta(seconds=60)
        else:
            starttime = intancesresp['Instances'][i]['Status']['Timeline']['CreationDateTime'] + timedelta(seconds=60)
        endtime=intancesresp['Instances'][i]['Status']['Timeline']['EndDateTime'] - timedelta(seconds=60)
        if starttime >= endtime:
            continue
        response = cwclient.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='CPUUtilization',
            Dimensions=[
                {
                    'Name': 'InstanceId',
                    'Value': instanceId
                },
            ],
            StartTime=starttime,
            EndTime=endtime,
            Period=period,
            Statistics=[
                'Average',
            ],
            Unit='Percent'
        )
        for metrics in response['Datapoints']:
            averages.append(metrics['Average'])
    return sum(averages)/len(averages)

def scanTags(tags):
    team = ""
    project = ""
    for tag in tags:
        if tag['Key'] == 'project':
            project = tag['Value']
        if tag['Key'] == 'team':
            team = tag['Value']
    return (team, project)

def get_cluster_cost_stats(clusterIds):
        """
        Gets Cluster Cost
        """
        cost_explorer = boto3.client('ce')
        cost = cost_explorer.get_cost_and_usage(
            TimePeriod={
                'Start': "2022-05-18",
                'End': "2022-05-19",
            },
            Metrics=['AmortizedCost'],
            Granularity='MONTHLY',
            Filter={
                "Tags": {
                    "Key": "aws:elasticmapreduce:job-flow-id",
                    "Values": clusterIds
                }
            }
        )

        total_cost = 0
        for monthly_cost in cost['ResultsByTime']:
            total_cost = total_cost + float(monthly_cost['Total']['AmortizedCost']['Amount'])
        return total_cost

print("Fetching the cluster ids for given date range")
clusterids= getClusterIds(datetime(2022, 5, 18), datetime(2022, 5, 19))
results={}
i = 0
print("Total clusters - " + str(len(clusterids)))
print("Scanning the team and project names")
while i < len(clusterids):
    try:
        clusterid = clusterids[i]
        print(clusterid)
        descCluster = client.describe_cluster(
            ClusterId=clusterid
        )
        (team, project) = scanTags(descCluster['Cluster']['Tags'])
        ppppp = team, project
        print("Cluster Id, team, project - " + clusterid+", "+team+", "+project)
        time.sleep(0.2)
        if project in results:
            clusters,item= results[project]
            clusters.append(clusterid)
        else:
            clusters=[]
            clusters.append(clusterid)
            results[project] = clusters,team
        i += 1
    except:
        print("wait")
        time.sleep(0.2)
        continue

print("Extracted all the clusters for given date range")
for project,item in results.items():
    averageCpuUtilization = 0
    averages = []
    clusters,team = item
    print("Team, Project: "+team+", "+project)

    for cluster in clusters:
        averages.append(getCpuUtilization(cluster))

    averageCpuUtilization=sum(averages)/len(averages)

    if averageCpuUtilization < 60:
        cost = get_cluster_cost_stats(clusters)
        percentSaving = ((60-averageCpuUtilization)/60)*100
        costSaved = ((cost*percentSaving)/100)
        if costSaved > 25 :
            pp.pprint("Team :" + team + "| Project: " + project + "| No. of clusters : " + str(
            len(averages)) + "| CPU Utilization : " + str(
            averageCpuUtilization) + "%| COST: " + str(cost) + "$| COST Can be saved: " + str(costSaved)+ "$")

            json = {
                "fields": {
                    "labels": ['Codefest2022', 'EMRCostSaving'],
                    "project":
                        {
                            "key": "PURE"
                        },
                    "summary": "[Codefest] EMR cost improvement",
                    "description": "Hi team, Eagle Eyes has found that your clusters are not optimised.\nThese are the results for the EMR clusters your team is using:\n\nDate Range: 18th May - 19th May 2022\n\nTeam: "+team+"\nProject: "+project+"\nNo. of Clusters: "+str(len(averages))+"\nCPU Utilisation: "+str(averageCpuUtilization)+"\nCost: $"+str(cost)+"\n\nAccording to the results of your report a total cost of *$"+str(costSaved)+" can be saved*.\n\nTip: Please consider switching to memory optimised nodes as cluster has high memory utilisation but low CPU utilisation\n\nYou can refer to [this |https://docs.google.com/document/d/1o24BHYq-x2lZSZkllv5iVtbdrgk08ouLkFngN-CJ3eI/edit]document for best practices for optimising your EMR cluster",
                    "issuetype": {
                        "name": "Story"
                    }
                }
            }

            headers = {
                'cookie': '_biz_uid=6be5d17707ba4e91ea61f07bc8d2442c; _mkto_trk=id:469-VBI-606&token:_mch-integralads.com-1630915044321-51843; _fbp=fb.1.1630915044379.1948465326; _biz_flagsA=%7B%22Version%22%3A1%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%2C%22Mkto%22%3A%221%22%7D; OptanonAlertBoxClosed=2021-09-06T11:25:20.671Z; _gcl_aw=GCL.1649417210.CjwKCAjwur-SBhB6EiwA5sKtjoMk4_tgAPdNfXN0TEygCtIGdPAUcv9FnWiAbOpj1T8cZQVmfGoqAxoCM38QAvD_BwE; _gcl_au=1.1.1217715853.1649417210; _biz_nA=32; _biz_pendingA=%5B%5D; OptanonConsent=isIABGlobal=false&datestamp=Wed+Apr+13+2022+17%3A29%3A08+GMT%2B0530+(India+Standard+Time)&version=6.31.0&landingPath=NotLandingPage&groups=C0004%3A0%2CC0003%3A0%2CC0001%3A1%2CC0002%3A0&AwaitingReconsent=false&isGpcEnabled=0&geolocation=IN%3BKA&hosts=&consentId=5e32d837-8678-42e4-8714-55fa7100dd3e&interactionCount=0; _ga_270881840=GS1.1.1649851134.14.0.1649851343.0; _ga_29B6GT2LBL=GS1.1.1651081843.1.1.1651081885.0; _ga_7P52HWG2RL=GS1.1.1652794583.1.1.1652794614.0; ajs_user_id=%2270a729d59081b4a7806e678e3b70d14798ba56b1%22; ajs_anonymous_id=%2222d26cb3-11d8-4aac-855b-1a42cc365100%22; _gid=GA1.2.317285429.1653235067; JiraSDSamlssoLoginV2=RESOLUTION_fd6d4a59-36f0-49b2-a31c-2786b19fe729%23%23%23apangaonkar%23%23%23http%3A%2F%2Fwww.okta.com%2Fexk13ucc08BiY0OPr2p7%23%23%235EWJ9; JSESSIONID=5643426B338F572AD5ED85B82AA2509F; atlassian.xsrf.token=BQ60-HELO-ABBC-JAHU_a73c0ff2ba769e738125d7a521e25e2ccae097e2_lin; _ga_LL8Z7CERHK=GS1.1.1653372530.22.1.1653373446.0; _ga=GA1.2.1266660238.1652791991; _gat_gtag_UA_98634279_1=1'}
            response = requests.post('https://jira.integralads.com/rest/api/2/issue', json=json, headers=headers)

            #print(json)
    else:
        print("CPU utilization is " + str(averageCpuUtilization) + " which is >= 60%, No cost optimization required!")