import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime

#def get_region():
#    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
#    response_json = r.json()
#    return response_json.get('region')

region_name = 'us-west-2'
keyid = 'AKIAIJS2VFNEIVPJAOPQ'
skey = 'zWLTf6l4oK5E011wf0fjmSiYGDqHtxHpvCXetX19'

def client():
    global emr
    emr = boto3.client('emr', region_name='us-west-2', aws_access_key_id='AKIAIJS2VFNEIVPJAOPQ', aws_secret_access_key='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')


def get_security_group_id(group_name, region_name='us-west-2', aws_access_key_id='AKIAIJS2VFNEIVPJAOPQ', aws_secret_access_key='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'):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']


def create_cluster(region_name='us-west-2', cluster_name='Airflow-' + str(datetime.now()), release_label='emr-5.9.0',
                   master_instance_type='m3.xlarge', num_core_nodes=2, core_node_instance_type='m3.2xlarge',
                  aws_access_key_id='AKIAIJS2VFNEIVPJAOPQ', aws_secret_access_key='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'):

    #emr_master_security_group_id = get_security_group_id('AirflowEMRMasterSG', region_name=region_name)
    #emr_slave_security_group_id = get_security_group_id('AirflowEMRSlaveSG', region_name=region_name)

    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'airflow_key_pair',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },

        VisibleToAllUsers=True,
        JobFlowRole='EmrEc2InstanceProfile',
        ServiceRole='EmrRole',
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'livy' },
            { 'Name': 'zeppelin' }
        ]
    )
    return cluster_response['JobFlowId']


def get_cluster_dns(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def get_cluster_status(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']


def wait_for_cluster_creation(cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
                                                