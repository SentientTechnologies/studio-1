import os
import json


from .util import timeit
from .local_artifact_store import LocalArtifactStore
from .storage_provider import StorageProvider


class LocalFilesProvider(StorageProvider):

    def __init__(self, db_config,
                 blocking_auth=True,
                 verbose=10):
        self.folder = os.path.abspath(os.path.expanduser(
            db_config['folder']
        ))

        store = LocalArtifactStore(self.folder)

        super(StorageProvider, self).__init__(
            db_config,
            blocking_auth=blocking_auth,
            store=store,
            verbose=verbose
        )

    @timeit
    def _get(self, key, shallow=False):
        path = os.path.join(self.folder, key)
        if os.path.isfile(path):
            with open(path) as f:
                data = json.loads(f.read())
            return data
        elif os.path.isdir(path) and shallow:
            return os.listdir(path)
        else:
            raise NotImplementedError

    @timeit
    def _set(self, key, value):
        filename = os.path.join(self.folder, key)
        path = os.path.dirname(filename)

        if not os.path.exists(path):
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(json.dumps(value))

    @timeit
    def _delete(self, key):
        self.store._delete_file(key)
