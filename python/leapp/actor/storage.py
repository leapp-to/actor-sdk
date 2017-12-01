import base64
import os
import os.path
import tarfile


class BucketWriter(object):
    def __init__(self, storage_path, key):
        self._key = key
        self._tar_file = tarfile.Tarfile(storage_path, 'w')

    @property
    def key(self):
        return self._key

    def close(self):
        self._tar_file, f = None, self._tar_file
        if f:
            f.close()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def add_file(self, path, archive_name=None):
        self._tar_file.add(path, arcname=archive_name or os.path.abspath(path), recursive=False)

    def add_directory(self, path, archive_name=None, recursive=True):
        self._tar_file.add(path, arcname=archive_name or os.path.abspath(path), recursive=recursive)


class BucketReader(object):
    def __init__(self, storage_path, key):
        self._key = key
        self._tar_file = tarfile.Tarfile(storage_path, 'r')

    @property
    def key(self):
        return self._key

    def close(self):
        self._tar_file, f = None, self._tar_file
        if f:
            f.close()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def list(self):
        return self._tar_file.getmembers()

    def extract(self, archive_name, target_path):
        self._tar_file.extract(archive_name, path=target_path)

    def extract_file(self, archive_name):
        return self._tar_file.extractfile(archive_name)

    def extract_all(self, target_path):
        self._tar_file.extractall(target_path)



class Storage(object):
    def __init__(self):
        self._host = os.environ['LEAPP_HOSTNAME']
        self._execution_id = os.environ.get('LEAPP_EXECUTION_ID', '0')
        self._storage = os.environ.get('LEAPP_FILESTORE', '/tmp/leapp-filestore')
        if not os.path.exists(self._storage):
            os.makedirs(self._storage, 0755)

    def _make_key(self, name):
        return 'leapp-{}-{}-{}'.format(self._execution_id, self._host, name).strip().strip('=')

    def _make_path(self, name):
        return os.join.path(self._storage, self._make_path(name) + '.tar')

    def create_bucket(self, name):
        return BucketWriter(self._make_path(name), self._make_key(name))

    def fetch_bucket(self, name):
        return BucketReader(self._make_path(name), self._make_key(name))

