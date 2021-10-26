import re

def convert_to_MiB(size_string):
    """
    Function to convert to data sizes to MiB from string 
    e.g. "10g", "10 GB"
    """
    # default if no units mentioned is MiB
    if size_string.isdigit():
        data_size = float(size_string)
    else:
        pattern = "([0-9]+) *([a-zA-z]+)"
        match = re.match(pattern, size_string.strip(' '))

        data_size = float(match.group(1))
        unit = match.group(2)

        if unit == 'kiB':
            data_size = data_size / 1024
        elif unit == 'GiB' or unit == 'g':
            data_size = data_size * 1024
        elif unit == 'GB':        
            data_size = data_size * 10**9 / 1024 / 1024
        elif unit == 'MB':        
            data_size = data_size * 10**6 / 1024 / 1024        
        elif unit == 'kB':
            data_size = data_size * 10**3 / 1024 / 1024
        elif unit == 'bytes' or unit == 'B':
            data_size = data_size / 1024 / 1024
    return data_size


def db_to_aws_configs(appobj):
    meta = appobj.sparkMetadata
    rt = 'spark.databricks.clusterUsageTags.'

    config = {
        "clusterConf": {
            "AvailabilityZone": meta[rt+'containerZoneId'],
            "InstanceGroupType": {
                "MASTER": {
                    "InstanceType": meta[rt+'driverNodeType'],
                    "InstanceCount": 1,
                    "TotalEBSSizePerInstance": 0,
                    "VolumesPerInstance": 1
                },
                "CORE": {
                    "InstanceType": meta[rt+'clusterNodeType'],
                    "InstanceCount": int(meta[rt+'clusterWorkers']),
                    "TotalEBSSizePerInstance": int(meta[rt+'clusterEbsVolumeCount'])*int(meta[rt+'clusterEbsVolumeSize']),
                    "VolumesPerInstance": int(meta[rt+'clusterEbsVolumeCount'])
                },
                "TASK": {
                    "InstanceType": None,
                    "InstanceCount": None,
                    "TotalEBSSizePerInstance": None,
                    "VolumesPerInstance": None
                }
            }
        },
        "region": meta[rt+'dataPlaneRegion'],
        'type': 'INSTANCE_GROUP'
    }

    return config