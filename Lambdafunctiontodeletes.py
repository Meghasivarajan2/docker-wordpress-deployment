import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # Get all running instance IDs
    running_instances = set()
    instance_paginator = ec2.get_paginator('describe_instances')

    for page in instance_paginator.paginate(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
    ):
        for reservation in page['Reservations']:
            for instance in reservation['Instances']:
                running_instances.add(instance['InstanceId'])

    # Get all self-owned snapshots
    snapshot_paginator = ec2.get_paginator('describe_snapshots')

    for page in snapshot_paginator.paginate(OwnerIds=['self']):
        for snapshot in page['Snapshots']:
            snapshot_id = snapshot['SnapshotId']
            volume_id = snapshot.get('VolumeId')

            try:
                # If snapshot has no volume → delete
                if not volume_id:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                    print(f"Deleted snapshot {snapshot_id} (no volume)")
                    continue

                # Check volume details
                volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
                attachments = volume.get('Attachments', [])

                # If volume is not attached → delete snapshot
                if not attachments:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                    print(f"Deleted snapshot {snapshot_id} (volume not attached)")
                    continue

                # Check if attached to any running instance
                attached_instance_ids = {att['InstanceId'] for att in attachments}

                if not attached_instance_ids & running_instances:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                    print(f"Deleted snapshot {snapshot_id} (not attached to running instance)")

            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                    print(f"Deleted snapshot {snapshot_id} (volume deleted)")
                else:
                    print(f"Error processing snapshot {snapshot_id}: {e}")
