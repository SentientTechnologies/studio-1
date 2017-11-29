import logging
import time
import os
import six
import re
from threading import Thread

from . import util, git_util, pyrebase
from .firebase_artifact_store import FirebaseArtifactStore
from .auth import get_auth
from .experiment import experiment_from_dict
from .tartifact_store import get_immutable_artifact_key
from .util import timeit
from .keyvalue_provider import KeyValueProvider

logging.basicConfig()


class StorageProvider(KeyValueProvider):
    """
    Abstract class for experiment metadata
    and artifact storage
    """

    def __init__(
            self,
            db_config,
            blocking_auth=True,
            verbose=10,
            store=None,
            compression=None):
        guest = db_config.get('guest')

        self.app = pyrebase.initialize_app(db_config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(verbose)

        self.compression = compression
        if self.compression is None:
            self.compression = db_config.get('compression')

        self.auth = None
        if not guest and 'serviceAccount' not in db_config.keys():
            self.auth = get_auth(self.app,
                                 db_config.get("use_email_auth"),
                                 db_config.get("email"),
                                 db_config.get("password"),
                                 blocking_auth)

        self.store = store

        if self.auth and not self.auth.expired:
            self.register_user(None, self.auth.get_user_email())

        self.max_keys = db_config.get('max_keys', 100)

    def _get_userid(self):
        userid = None
        if self.auth:
            userid = self.auth.get_user_id()
        userid = userid if userid else 'guest'
        return userid

    def _get_user_keybase(self, userid=None):
        if userid is None:
            userid = self._get_userid()

        return "users/" + userid + "/"

    def _get_experiments_keybase(self, userid=None):
        return self._get_user_keybase(userid)

    def add_experiment(self, experiment, userid=None, compression=None):
        self._delete(self._get_experiments_keybase() + experiment.key)
        experiment.time_added = time.time()
        experiment.status = 'waiting'

        compression = compression if compression else self.compression

        if 'local' in experiment.artifacts['workspace'].keys() and \
                os.path.exists(experiment.artifacts['workspace']['local']):
            experiment.git = git_util.get_git_info(
                experiment.artifacts['workspace']['local'])

        for tag, art in six.iteritems(experiment.artifacts):
            if art['mutable']:
                art['key'] = self._get_experiments_keybase() + \
                    experiment.key + '.data/' + tag + '.tar' + \
                    util.compression_to_extension(compression)
            else:
                if 'local' in art.keys():
                    # upload immutable artifacts
                    art['key'] = self.store.put_artifact(art)
                elif 'hash' in art.keys():
                    art['key'] = get_immutable_artifact_key(
                        art['hash'],
                        compression=compression
                    )

            key = art.get('key')
            if key is not None:
                art['qualified'] = self.store.get_qualified_location(key)
                art['bucket'] = self.store.get_bucket()
            elif art.get('qualified'):
                qualified = art.get('qualified')
                bucket = re.search('(?<=://)[^/]+(?=/)', qualified).group(0)
                if bucket.endswith('.com'):
                    bucket = re.search(
                        '(?<=' + re.escape(bucket) + '/)[^/]+(?=/)',
                        qualified
                    ).group(0)

                key = re.search('(?<=' + bucket + '/).+\Z', qualified).group(0)
                art['bucket'] = bucket
                art['key'] = key

        userid = userid if userid else self._get_userid()
        experiment.owner = userid

        experiment_dict = experiment.__dict__.copy()

        self._set(self._get_experiments_keybase() + experiment.key,
                  experiment_dict)

        self.checkpoint_experiment(experiment, blocking=True)
        self.logger.info("Added experiment " + experiment.key)

    def get_user_experiments(self, userid=None, blocking=True):
        experiment_keys = self._get(
            self._get_user_keybase(userid), shallow=True)
        if not experiment_keys:
            experiment_keys = []

        return experiment_keys

    def get_users(self):
        user_ids = self._get('users/', shallow=True)
        return user_ids

    def can_write(self, key=None, user=None):
        # TODO implemetent ACL's
        return True
        assert key is not None
        user = user if user else self._get_userid()

        experiment = self._get(
            self._get_experiments_keybase() + key)

        if experiment:
            owner = experiment.get('owner')
            if owner is None or owner == 'guest':
                return True
            else:
                return (owner == user)
        else:
            return True

    def can_read(self, path, user=None):
        return True

    def browse(self, path):
        data = self._get(path, shallow=True)
        return data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.app:
            self.app.requests.close()
        if self.store:
            self.store.__exit__()
