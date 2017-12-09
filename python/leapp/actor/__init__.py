import atexit
import json
import sys
from .storage import Storage


def _process_commands(actor):
    if '--dump-yaml' in sys.argv:
        actor.dump(sys.stdout)
        sys.exit(0)
    elif '--ansible-run' in sys.argv:
        sys.argv.pop(sys.argv.index('--ansible-run'))
        json_path = sys.argv.pop(1)
        actor.fun(AnsibleNonNativeModuleChannels(json_path))
    elif '--leapp-run' in sys.argv:
        sys.argv.pop(sys.argv.index('--leapp-run'))
        actor.fun(StandardInputChannels())


class ActorInfo(object):
    def __init__(self, fun, name, description, inputs, outputs, tags):
        self.fun = fun
        self.name = name
        self.description = description or 'No description has been provided for this actor'
        self.inputs = inputs or {}
        self.outputs = outputs or {}
        self.tags = tags or ()

    def dump(self, fobj, ):
        fobj.write('---\n')
        if self.description:
            fobj.write('description: |\n')
            fobj.write('    ' + self.description.replace('\n', '\n    ').strip() + '\n')
        if self.tags:
            fobj.write('tags:\n')
            for tag in self.tags:
                fobj.write('- {}\n'.format(tag))
        if self.inputs:
            fobj.write('inputs:\n')
            for name, schema in self.inputs.items():
                fobj.write('- name: {}\n  type:\n    name: {}\n'.format(name, schema))
        if self.outputs:
            fobj.write('outputs:\n')
            for name, schema in self.outputs.items():
                fobj.write('- name: {}\n  type:\n    name: {}\n'.format(name, schema))
        fobj.write('execute:\n  executable: /usr/bin/python\n')
        fobj.write('  script-file: actor.py\n  arguments:\n  - "--leapp-run"\n')


def actorize(name, description=None, inputs=None, outputs=None, tags=None):
    def wrapper(f):
        _process_commands(ActorInfo(f, name, description, inputs, outputs, tags))
        return f
    return wrapper


class Channel(object):
    def __init__(self, owner, name):
        self._owner = owner
        self._name = name
        self.__data = None
        self._iter = None

    @property
    def _data(self):
        if self.__data is None:
            data = self._owner.raw(self._name)
            if not data:
                raise AttributeError()
            self.__data = list(data)
        return self.__data

    def __iter__(self):
        self._iter = iter(self._data)
        return self._iter

    def __next__(self):
        return next(self._iter)

    def pop(self):
        return self._data.pop()

    def push(self, message, tags=()):
        self._owner.push_message(self._name, message, tags=tags)


class ChannelsBase(object):
    def __init__(self, in_data):
        self._stdout = sys.stdout
        sys.stdout = sys.stderr
        self._in_data = in_data
        self._out_data = {}
        self._channels = {name: Channel(self, name) for name in self._in_data.keys()}
        self._producer = []
        atexit.register(self._write)

    def add_producer(self, producer):
        self._producer.append(producer)

    def exists(self, name):
        return name in self._in_data or name in self._out_data

    def _write(self):
        for producer in self._producer:
            for channel, messages in producer.get_messages().items():
                for message in messages:
                    self.push_message(channel, message)
        json.dump(self._out_data, self._stdout)
        self._stdout.write('\n')

    def raw(self, channel):
        return self._in_data.get(channel)

    def push_message(self, channel, message, tags=()):
        self._out_data.setdefault(channel, []).append(message)

    def __getattr__(self, name):
        return self._channels.setdefault(name, Channel(self, name))


class AnsibleNonNativeModuleChannels(ChannelsBase):
    def __init__(self, file_path):
        input_data = {}
        with open(file_path, 'r') as f:
            input_data = json.load(f)
        super(AnsibleNonNativeModuleChannels, self).__init__(input_data)


class StandardInputChannels(ChannelsBase):
    def __init__(self):
        input_data = {}
        if not sys.stdin.isatty():
            data = sys.stdin.read() or '{}'
            input_data = json.loads(data)
        super(StandardInputChannels, self).__init__(input_data)
