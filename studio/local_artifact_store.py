import os
import shutil
import logging

from .tartifact_store import TartifactStore

logging.basicConfig()


class LocalArtifactStore(TartifactStore):

    def __init__(self, folder, compression=None, verbose=logging.DEBUG):
        self.folder = os.path.abspath(os.path.expanduser(folder))
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

        super(LocalArtifactStore, self).__init__(
            compression=None,
            verbose=verbose,
            measure_timestamp_diff=False
        )

    def _upload_file(self, key, local_path):
        store_filename = self._get_path(key)
        store_path = os.path.dirname(store_filename)
        if not os.path.exists(store_path):
            try:
                os.makedirs(store_path)
            except OSError:
                pass

        shutil.copy(local_path, store_filename)

    def _download_file(self, key, local_path):
        shutil.copy(self._get_path(key), local_path)

    def _delete_file(self, key):
        store_path = self._get_path(key)

        if not os.path.exists(store_path):
            return

        if os.path.isfile(store_path):
            os.remove(store_path)
        else:
            shutil.rmtree(store_path)

    def _get_file_url(self, key, method=None):
        return "file://" + os.path.join(self.folder, key)

    def _get_file_timestamp(self, key):
        return os.path.getmtime(self._get_path(key))

    def _get_path(self, key):
        return os.path.join(self.folder, key)

    def get_qualified_location(self, key):
        return self._get_file_url(key)

    def get_bucket(self):
        return self.folder
