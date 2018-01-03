#!/usr/bin/env python

import rados
import time
import json
import os
import sys
import traceback

import ceph_iscsi_config.settings as settings
from ceph_iscsi_config.utils import get_time

class ConfigTransaction(object):

    def __init__(self, cfg_type, element_name, txn_action='add', initial_value=None):

        self.type = cfg_type
        self.action = txn_action
        self.item_name = element_name

        init_state = {} if initial_value is None else initial_value
        self.item_content = init_state

    def __repr__(self):
        return str(self.__dict__)


class CephCluster(object):

    def __init__(self):

        self.error = False
        self.error_msg = ''
        self.cluster = rados.Rados(conffile=settings.config.cephconf,
                                   conf=dict(keyring="/var/lib/ceph/etc/ceph/{}".format(settings.config.gateway_keyring)))
        try:
            self.cluster.connect()
        except rados.Error as err:
            self.error = True
            self.error_msg = "Unable to connect to the cluster (keyring missing?) - {}".format(err)

    def __del__(self):
        self.cluster.shutdown()

    def shutdown(self):
        self.cluster.shutdown()


class Config(object):

    seed_config = {
                    "disks": {},
                    "gateways": {},
                    "clients": {},
                    "groups": {},
                    "version": 3,
                    "epoch": 0,
                    "created": '',
                    "updated": ''
                    }

    lock_time_limit = 30

    def __init__(self, logger, cfg_name='gateway.conf', pool='rbd'):
        self.logger = logger
        self.config_name = cfg_name
        self.pool = pool
        self.ceph = None
        self.error = False
        self.reset = False
        self.error_msg = ""
        self.txn_list = []
        self.config_locked = False

        self.ceph = CephCluster()
        if self.ceph.error:
            self.error = True
            self.error_msg = self.ceph.error_msg
            return

        self.config = self.get_config()
        self.changed = False

    def _read_config_object(self, ioctx):
        """
        Return config string from the config object. The string is checked to see if it's valid
        json. If it's not the read is likely to be a against the object while it's being updated by another
        host - if this happens, we wait and reread until we get valid json.
        :param ioctx: rados ioctx
        :return: (str) current string.
        """

        try:
            size, mtime = ioctx.stat(self.config_name)
        except rados.ObjectNotFound:
            self.logger.debug("_read_config_object object not found")
            cfg_str = ''
        else:
            self.logger.debug("_read_config_object reading the config object")
            size += 1
            cfg_str = ioctx.read(self.config_name, length=size)
            if cfg_str:
                valid = False
                while not valid:
                    try:
                        cfg_js = json.loads(cfg_str)
                    except ValueError:
                        #
                        self.logger.debug("_read_config_object not valid json, rereading")
                        time.sleep(1)
                        size, mtime = ioctx.stat(self.config_name)
                        cfg_str = ioctx.read(self.config_name, length=size)
                    else:
                        valid = True

        return cfg_str

    def _get_ceph_config(self):

        cfg_dict = {}

        try:
            self.logger.debug("(_get_rbd_config) Opening connection to {} pool".format(self.pool))
            ioctx = self.ceph.cluster.open_ioctx(self.pool)       # open connection to pool
        except rados.ObjectNotFound:
            self.error = True
            self.error_msg = "'{}' pool does not exist!".format(self.pool)
            self.logger.error("(Config._get_rbd_config) {}".format(self.error_msg))
            return {}
        else:
            self.logger.debug("(_get_rbd_config) connection opened")

        cfg_data = self._read_config_object(ioctx)
        ioctx.close()

        if not cfg_data:
            # attempt to read the object got nothing which means it's empty
            # so we seed the object
            self.logger.debug("(_get_rbd_config) config object is empty..seeding it")
            self._seed_rbd_config()
            if self.error:
                self.logger.error("(Config._get_rbd_config) Unable to seed the config object")
                return {}
            else:
                cfg_data = json.dumps(Config.seed_config)

        self.logger.debug("(_get_rbd_config) config object contains '{}'".format(cfg_data))

        cfg_dict = json.loads(cfg_data)

        return cfg_dict

    def get_config(self):
        return self._get_ceph_config()

    def lock(self):

        ioctx = self.ceph.cluster.open_ioctx(self.pool)

        secs = 0
        self.logger.debug("config.lock attempting to acquire lock on {}".format(self.config_name))
        while secs < Config.lock_time_limit:
            try:
                ioctx.lock_exclusive(self.config_name, 'lock', 'config')
                self.config_locked = True
                break
            except (rados.ObjectBusy, rados.ObjectExists):
                self.logger.debug("(Config.lock) waiting for excl lock on {} object".format(self.config_name))
                time.sleep(1)
                secs += 1

        if secs >= Config.lock_time_limit:
            self.error = True
            self.error_msg = ("Timed out ({}s) waiting for excl "
                              "lock on {} object".format(Config.lock_time_limit, self.config_name))
            self.logger.error("(Config.lock) {}".format(self.error_msg))

        ioctx.close()

    def unlock(self):
        ioctx = self.ceph.cluster.open_ioctx(self.pool)

        self.logger.debug("config.unlock releasing lock on {}".format(self.config_name))
        try:
            ioctx.unlock(self.config_name, 'lock', 'config')
            self.config_locked = False
        except Exception as e:
            self.error = True
            self.error_msg = ("Unable to unlock {} - {}".format(self.config_name,
                                                                traceback.format_exc()))
            self.logger.error("(Config.unlock) {}".format(self.error_msg))

        ioctx.close()

    def _seed_rbd_config(self):

        ioctx = self.ceph.cluster.open_ioctx(self.pool)

        self.lock()
        if self.error:
            return

        # if the config object is empty, seed it - if not just leave as is
        cfg_data = self._read_config_object(ioctx)
        if not cfg_data:
            self.logger.debug("_seed_rbd_config found empty config object")
            seed_now = Config.seed_config
            seed_now['created'] = get_time()
            seed = json.dumps(seed_now, sort_keys=True, indent=4, separators=(',', ': '))
            ioctx.write_full(self.config_name, seed)
            ioctx.set_xattr(self.config_name, "epoch", "0")
            self.changed = True

        self.unlock()

    def refresh(self):
        self.logger.debug("config refresh - current config is {}".format(self.config))
        self.config = self.get_config()

    def add_item(self, cfg_type, element_name=None, initial_value=None):
        now = get_time()

        if element_name:
            # ensure the initial state for this item has a 'created' date/time value
            if isinstance(initial_value, dict):
                if 'created' not in initial_value:
                    initial_value['created'] = now

            if initial_value is None:
                init_state = {"created": now}
            else:
                init_state = initial_value

            self.config[cfg_type][element_name] = init_state

            if isinstance(init_state, str) and 'created' not in self.config[cfg_type]:
                self.config[cfg_type]['created'] = now
                # add a separate transaction to capture the creation date to the section
                txn = ConfigTransaction(cfg_type, 'created', initial_value=now)
                self.txn_list.append(txn)

        else:
            # new section being added to the config object
            self.config[cfg_type] = initial_value
            init_state = initial_value
            txn = ConfigTransaction(cfg_type, None, initial_value=initial_value)
            self.txn_list.append(txn)

        self.logger.debug("(Config.add_item) config updated to {}".format(self.config))
        self.changed = True

        txn = ConfigTransaction(cfg_type, element_name, initial_value=init_state)
        self.txn_list.append(txn)

    def del_item(self, cfg_type, element_name):
        self.changed = True
        del self.config[cfg_type][element_name]
        self.logger.debug("(Config.del_item) config updated to {}".format(self.config))

        txn = ConfigTransaction(cfg_type, element_name, 'delete')
        self.txn_list.append(txn)

    def update_item(self, cfg_type, element_name, element_value):
        now = get_time()

        if element_name:
            current_values = self.config[cfg_type][element_name]
            self.logger.debug("prior to update, item contains {}".format(current_values))
            if isinstance(element_value, dict):
                merged = current_values.copy()
                new_dict = element_value
                new_dict['updated'] = now
                merged.update(new_dict)
                element_value = merged.copy()

            self.config[cfg_type][element_name] = element_value
        else:
            # update to a root level config element, like version
            self.config[cfg_type] = element_value

        self.logger.debug("(Config.update_item) config is {}".format(self.config))
        self.changed = True
        self.logger.debug("update_item: type={}, item={}, update={}".format(cfg_type, element_name, element_value))

        txn = ConfigTransaction(cfg_type, element_name, 'add')
        txn.item_content = element_value
        self.txn_list.append(txn)

    def _commit_rbd(self, post_action):

        ioctx = self.ceph.cluster.open_ioctx(self.pool)

        if not self.config_locked:
            self.lock()
            if self.error:
                return

        # reread the config to account for updates made by other systems
        # then apply this hosts update(s)
        current_config = json.loads(self._read_config_object(ioctx))
        for txn in self.txn_list:

            self.logger.debug("_commit_rbd transaction shows {}".format(txn))
            if txn.action == 'add':         # add's and updates
                if txn.item_name:
                    current_config[txn.type][txn.item_name] = txn.item_content
                else:
                    current_config[txn.type] = txn.item_content

            elif txn.action == 'delete':
                del current_config[txn.type][txn.item_name]
            else:
                self.error = True
                self.error_msg = "Unknown transaction type ({}} encountered in _commit_rbd".format(txn.action)

        if not self.error:
            if self.reset:
                current_config["epoch"] = 0
            else:
                current_config["epoch"] += 1        # Python will switch from plain to long int automagically

            now = get_time()
            current_config['updated'] = now
            config_str = json.dumps(current_config)
            self.logger.debug("_commit_rbd updating config to {}".format(config_str))
            config_str_fmtd = json.dumps(current_config, sort_keys=True, indent=4, separators=(',', ': '))
            ioctx.write_full(self.config_name, config_str_fmtd)
            ioctx.set_xattr(self.config_name, "epoch", str(current_config["epoch"]))
            del self.txn_list[:]                # emtpy the list of transactions

        self.unlock()
        ioctx.close()

        if post_action == 'close':
            self.ceph.shutdown()

    def commit(self, post_action='close'):
        self._commit_rbd(post_action)


def ansible_control():
    """
    establish whether ansible modules are in the current path to determine whether the code is called
    through ansible, or directly through a module import. This is done by looking at the call stack, and
    relies on the main method in the ansible custom module being prefixed by 'ansible' e.g. ansible_main()
    :return: Boolean
    """

    return sys._getframe(2).f_code.co_name.startswith('ansible')



def main():
    pass

if __name__ == '__main__':

    main()
