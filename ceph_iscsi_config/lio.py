#!/usr/bin/env python


import os

from rtslib_fb import root
from rtslib_fb.utils import RTSLibError, RTSLibNotInCFS
from ceph_iscsi_config.common import Config

__author__ = 'Paul Cuzner, Michael Christie'


class LIO(object):

    def __init__(self, config_object):
        self.lio_root = root.RTSRoot()
        self.error = False
        self.error_msg = ''
        self.changed = False
        self.config = config_object

    def drop_lun_maps(self, config, update_config):

        for iqn in self.config.config['targets'].keys():
            disk_keys = self.config.config['targets'][iqn]['disks'].keys()

            for stg_object in self.lio_root.storage_objects:
                if stg_object.name in disk_keys:

                    # this is an rbd device that's in the config object,
                    # so remove it
                    try:
                        stg_object.delete()
                    except RTSLibError as err:
                        self.error = True
                        self.error_msg = err
                    else:
                        self.changed = True

                        if update_config:
                            # update the disk item to remove the wwn info
                            image_metadata = config.config['disks'][stg_object.name]
                            image_metadata['wwn'] = ''
                            config.update_item(iqn, "disks",
                                               stg_object.name,
                                               image_metadata)


class Gateway(LIO):

    def __init__(self, config_object):
        LIO.__init__(self, config_object)

        self.config = config_object

    def session_count(self):
        return len(list(self.lio_root.sessions))

    def drop_targets(self, this_host):
        for iqn in self.config.config['targets'].keys():
            if this_host in self.config.config['targets'][iqn]['gateways']:

                lio_root = root.RTSRoot()
                for tgt in lio_root.targets:
                    if tgt.wwn == iqn:
                        tgt.delete()
                        self.changed = True
