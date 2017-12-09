import os.path


class FSRegistryNode(object):
    def __init__(self, name, paths=None):
        self.name = name
        self._paths = paths or []

    def add_file(self, path):
        if os.path.isfile(path):
            self._paths.append(path)

    def add_directory(self, path, recursive=True, filterer=None):
        self._paths.append(path)

    def get_base_path(self):
        return '/'

    def encode(self):
        return {
            'name': self.name,
            'entities': self._paths}


class FSRegistry(object):
    def __init__(self, channels):
        self._received = {}
        if channels.raw('filesystem_transfer_nodes'):
            self._received = {n['name']: n for n in channels.filesystem_transfer_nodes}
        self._nodes = {}

    def get_messages(self):
        return {
            'filesystem_transfer_nodes': [node.encode() for node in self._nodes.values()]
        }

    def create_node(self, name):
        return self._nodes.setdefault(name, FSRegistryNode(name))

    def fetch_node(self, name):
        return FSRegistryNode(name, paths=self._received[name])
