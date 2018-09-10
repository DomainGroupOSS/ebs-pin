import boto3
import botocore
import backoff
import json
import time
import math
import arrow
import yaml
import sh
import os

EBS_PIN_ID = 'EBS-PIN-ID'
AWS_DEVICE = 'aws_device'
MOUNT_DIR = 'mount_dir'
OS_DEVICE = 'os_device'


def backoff_hdlr(details):
    print("Boto error happens here rarely due to network issues or permission issues, but logical errors. "
          "For example, try to delete an old volume with the same {tag}. When you see this error happening and "
          "this script is trying hard to retry, please double check if no human error has occured. ".format(tag=EBS_PIN_ID))


class EBSManager(object):
    def __init__(self, logger, id, aws_device, os_device, directory, region,
                 availability_zone, instance_id, instance_type,
                 volume_type='gp2',
                 init_size=20, create_wait_timeout=900, mark_file='.ebs-pin.yaml'):
        self.id = id
        self.aws_device = aws_device
        self.os_device = os_device
        self.directory = directory
        self.availability_zone = availability_zone
        self.region = region
        self.instance_id = instance_id
        self.instance_type = instance_type
        self.init_size = init_size
        self.volume_type = volume_type
        self.create_wait_timeout = create_wait_timeout
        self.logger = logger
        self.mark_file = mark_file
        self.client = boto3.Session().client('ec2', self.region)
        self.resource = boto3.Session().resource('ec2', self.region)

    @backoff.on_exception(backoff.expo,
                          botocore.exceptions.ClientError,
                          max_time=60,
                          on_backoff=backoff_hdlr)
    def attach_volume(self):
        self.logger.info("Checking existing mount status")
        volume = self._get_attached_volume()
        if volume is not None:
            self.logger.info(
                "System already mounted volume with %s => %s.", EBS_PIN_ID, self.id)
            return
        # check there is already a volume exists
        self.logger.info("Searching for volume %s => %s", EBS_PIN_ID, self.id)
        volume = self._get_latest_volume_available()
        self.logger.debug("Found volume: %s", volume)
        new_create = False
        if volume is None:
            volume = self._create_new_volume()
            new_create = True
        volume = self._attach_volume(volume)
        if not volume:
            raise Exception("Failed to attach volume.")
        if new_create:
            self.logger.info("Initialize %s with format ext4",
                             self.os_device)
            sh.bash('-c', '''(
echo o # Create a new empty DOS partition table
echo n # Add a new partition
echo p # Primary partition
echo 1 # Partition number
echo   # First sector (Accept default: 1)
echo   # Last sector (Accept default: varies)
echo w # Write changes
) | sudo fdisk {os_device}'''.format(os_device=self.os_device))
            sh.bash(
                '-c', 'sudo mkfs.ext4 {os_device}'.format(os_device=self.os_device))
        self.logger.info("mounting %s to %s",
                         self.os_device, self.directory)
        sh.bash('-c', 'if [[ ! -d {dir} ]]; then sudo mkdir {dir}; fi'.format(
            dir=self.directory
        ))
        sh.bash('-c', 'sudo mount {virtual_dvice} {dir}'.format(
            virtual_dvice=self.os_device,
            dir=self.directory
        ))
        sh.bash('-c', 'sudo chmod 777 {dir}'.format(
            dir=self.directory
        ))
        # validate if mark matches. If not fail. Else, udpate the mark
        mark_file = os.path.join(self.directory, self.mark_file)
        if os.path.isfile(mark_file):
            with open(mark_file) as f:
                existing_mark = yaml.load(f)
                if existing_mark.get(EBS_PIN_ID) != self.id:
                    raise Exception("The {id} from the attached volume ({volume_pin_id}) does not match this instance's'"
                                    " value ({instance_pin_id}), quit".format(
                                        id=EBS_PIN_ID, volume_pin_id=existing_mark.get(EBS_PIN_ID), instance_pin_id=self.id)
                                    )
        self.logger.info(
            "Mark the newly mounted volume with %s => %s", EBS_PIN_ID, self.id)
        with open(mark_file, 'w+') as newf:
            new_mark_obj = {
                EBS_PIN_ID: self.id,
            }
            yaml.dump(new_mark_obj, newf, default_flow_style=False)

    @backoff.on_exception(backoff.expo,
                          botocore.exceptions.ClientError,
                          max_time=60,
                          on_backoff=backoff_hdlr)
    def _get_latest_volume_available(self, cleanup=True):
        # when cleaning up, delete the older volumes created by time
        filters = [
            {"Name": 'tag-key',   "Values": [EBS_PIN_ID]},
            {"Name": 'tag-value', "Values": [self.id]}
        ]
        self.logger.debug(
            "Search for volume with tag %s, value %s", EBS_PIN_ID, self.id)
        volumes = self.client.describe_volumes(Filters=filters)['Volumes']
        if len(volumes) == 0:
            self.logger.debug("No such volume")
            return None
        volumes = sorted(
            volumes, key=lambda ss: ss['CreateTime'], reverse=True)  # todo: sort by attach time tag first.
        volume = volumes[0]
        result = None
        if volume['State'] == 'available' and volume['AvailabilityZone'] == self.availability_zone \
                or self.instance_id in [att.get('InstanceId') for att in volume.get('Attachments', [])]:
                # if available in this zone, or already attached to THIS instance
            result = volumes.pop(0)
            self.logger.debug("Found the newest one: %s", result)
        elif volume['State'] == 'available' and volume['AvailabilityZone'] != self.availability_zone:
            # if available in other zones, take a snapshot out of it.
            # Then later the volume will be deleted
            self._take_snapshot(volume, wait=True, cleanup=False)
        # Try to delete the rest
        if cleanup:
            self.logger.info(
                "If there are, clean up unused older volumes with %s => %s", EBS_PIN_ID, self.id)
            for v in self._find_should_deleted_volumes(volumes):
                self.logger.info("Deleting %s", v.get('VolumeId'))
                try:
                    self.client.delete_volume(VolumeId=v['VolumeId'])
                except Exception as e:
                    self.logger.warn("Exception %s, ignore and continue", e)
        return result

    def _get_latest_snapshot(self, cleanup=True):
        filters = [
            {"Name": 'tag-key',   "Values": [EBS_PIN_ID]},
            {"Name": 'tag-value', "Values": [self.id]},
        ]
        self.logger.info("Searching for snapshot Tag %s => %s",
                         EBS_PIN_ID, self.id)
        snapshots = self.client.describe_snapshots(Filters=filters)[
            'Snapshots']
        if len(snapshots) == 0:
            self.logger.info("No such snapshot")
            return None
        snapshots = sorted(
            snapshots, key=lambda ss: ss['StartTime'], reverse=True)
        result = snapshots.pop(0)
        if result.get('State') != 'completed':
            self.logger.error(
                "The snapshot we are looking for is not at completed state but %s. Something is wrong with previous "
                " snapshotting or you are snapshotting too fast? Please fix it manually. ", result.get('status'))
            raise Exception("Illegal Status for snapshot %s" %
                            result['SnapshotId'])
        if cleanup:
            self.logger.info(
                "If there are, will clean up snapshots that is no longer needed")
            for snapshot in self._find_should_deleted_snapshots(snapshots):
                self.logger.info("Deleting snapshot %s",
                                 snapshot['SnapshotId'])
                try:
                    self.client.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
                except Exception as e:
                    self.logger.warn("Exception %s, ignore and continue.", e)
        return result

    def _create_new_volume(self):
        self.logger.info('Creating a new volume out of possible snapshot')
        snapshot = self._get_latest_snapshot()
        if snapshot is None:
            self.logger.info('Creating a new volume.')
            volume = self.client.create_volume(
                Size=self.init_size,
                AvailabilityZone=self.availability_zone,
                VolumeType=self.volume_type
            )
        else:
            self.logger.info(
                'Creating a volume out of snapshot %s', snapshot['SnapshotId'])
            volume = self.client.create_volume(
                Size=self.init_size,
                SnapshotId=snapshot['SnapshotId'],
                AvailabilityZone=self.availability_zone,
                VolumeType=self.volume_type
            )
        self._tag_volume(volume['VolumeId'], [
            {'Key': 'CreateTime', 'Value': str(arrow.utcnow())},
        ])
        self._wait_volume(volume, wait_state='available')
        return volume

    def _get_attached_volume(self):
        if not os.path.isfile(os.path.join(self.directory, self.mark_file)):
            self.logger.info(
                "This instance does not have EBS Pinned volume mounted")
            return None
        found_volume = None
        for volume in self.resource.Instance(self.instance_id).volumes.all():
            self.logger.debug("Checking volume %s", str(volume))
            for tag in volume.tags:
                if tag.get('Key') == EBS_PIN_ID and tag.get('Value') == self.id:
                    found_volume = {
                        'VolumeId': volume.volume_id,
                        'CreateTime': volume.create_time
                    }
                    self._tag_volume(found_volume['VolumeId'])
                    return found_volume
        self.logger.info(
            'All the volume attached are not related to %s', self.id)
        return None

    @backoff.on_exception(backoff.expo,
                          botocore.exceptions.ClientError,
                          max_time=60)
    def create_snapshot(self, wait=False, cleanup=False):
        self.logger.debug("Checking volumes")
        volume = self._get_attached_volume()
        if not volume:
            self.logger.error('No value to snapshot')
            return
        self.logger.info("Creating snapshot for %s", volume['VolumeId'])
        self._take_snapshot(volume, wait=wait, cleanup=cleanup)

    def _take_snapshot(self, volume, wait=False, cleanup=False):
        try:
            if cleanup:
                self.logger.info(
                    "Will do some additional cleanup for the old snapshots")
                self._get_latest_snapshot(cleanup=True)

            timestamp = str(arrow.utcnow())
            additional_volume_tags = [
                {'Key': 'LastSnapthotTime', 'Value': timestamp},
            ]
            additional_snapshot_tags = [
                {'Key': 'CreateTime', 'Value':  timestamp},
            ]
            snapshot = self.client.create_snapshot(VolumeId=volume['VolumeId'])
            self.logger.info("New snapshot: %s for volume: %s",
                             snapshot['SnapshotId'], volume['VolumeId'])
            self._tag_snapshot(
                snapshot['SnapshotId'], additional_snapshot_tags)
            self._tag_volume(volume['VolumeId'], additional_volume_tags)
            if wait:
                self.logger.info(
                    "Wait for snapshot %s in completed state...", snapshot['SnapshotId'])
                num, wait = 0, 5
                retry_times = math.ceil(self.create_wait_timeout/wait)
                while True:
                    if num >= retry_times:
                        return False
                    try:
                        state = self.client.describe_snapshots(
                            SnapshotIds=[snapshot['SnapshotId']])['Snapshots'][0]['State']
                        if state == 'completed':
                            return True
                    except Exception as e:
                        self.logger.exception(e)
                    finally:
                        time.sleep(wait)
                        num += 1
                return snapshot
        except Exception as e:
            self.logger.exception(e)
            return None

    def _wait_volume(self, volume, wait_state='attached'):
        self.logger.debug("Wait for volume %s to be %s, max %ss",
                          volume['VolumeId'], wait_state, self.create_wait_timeout)
        num, wait = 0, 5
        retry_times = math.ceil(self.create_wait_timeout/wait)
        while True:
            if num >= retry_times:
                return False
            try:
                state = self.client.describe_volumes(VolumeIds=[volume['VolumeId']])[
                    'Volumes'][0]['State']
                self.logger.debug("Now: %s", state)
                if state == wait_state:
                    return True
            except Exception as e:
                self.logger.exception(e)
            finally:
                time.sleep(wait)
                num += 1

    def _attach_volume(self, volume):  # retry until finished
        try:
            # Oh, shit, let's take a snapshot immediately.
            if self.availability_zone != volume['AvailabilityZone']:
                self._take_snapshot(volume, wait=True)
                volume = self._create_new_volume()
            if not self.instance_id in [
                att.get('InstanceId') for att in volume.get('Attachments', [])
            ]:  # not attached to this instance yet
                self.logger.info(
                    "Attaching volume %s to this instance %s", volume['VolumeId'], self.instance_id)
                self.client.attach_volume(
                    VolumeId=volume['VolumeId'],
                    InstanceId=self.instance_id,
                    Device=self.aws_device
                )
                self._wait_volume(volume, 'in-use')
            return volume
        except:
            self.logger.exception("Failed to mount volume")
            return None

    def _find_should_deleted_snapshots(self, snapshots):
        # todo: apply cleanup strategy here. default, keep last 3
        return snapshots[3:]

    def _find_should_deleted_volumes(self, volumes):
        # todo: apply cleanup strategy here. default, clean all. As cross zone volume will cause data version confusion.
        return volumes

    def _tag_volume(self, volume, extra_tags=()):
        try:
            tags = [
                {'Key': 'Name', 'Value': '%s:%s:%s' % (
                    self.instance_id, self.aws_device, self.directory)},
                {'Key': EBS_PIN_ID, 'Value': self.id},
            ] + list(extra_tags)

            return self.client.create_tags(
                Resources=[volume],
                Tags=tags
            )
        except:
            self.logger.exception("Failed to create tags")
            return None

    def _tag_snapshot(self, snapshot_id, extra_tags=()):
        try:
            tags = [
                {'Key': 'Name', 'Value': '%s:%s:%s' % (
                    self.instance_id, self.aws_device, self.directory)},
                {'Key': EBS_PIN_ID, 'Value': self.id},
            ] + list(extra_tags)
            return self.client.create_tags(
                Resources=[snapshot_id],
                Tags=tags
            )
        except:
            self.logger.exception("Failed to create_tags %s" % tags)
            return None
