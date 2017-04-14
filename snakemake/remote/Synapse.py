__author__ = "Kenneth Daily"
__copyright__ = "Copyright 2017, Kenneth Daily"
__email__ = "kenneth.daily@sagebase.org"
__license__ = "MIT"

import os, re
from contextlib import contextmanager

# module-specific
from snakemake.remote import AbstractRemoteProvider, AbstractRemoteObject
from snakemake.exceptions import SynapseFileException, WorkflowError
import snakemake.io

try:
    # third-party modules
    import synapseclient
except ImportError as e:
    raise WorkflowError("The Python 3 package 'synapseclient'" +
        "need to be installed to use Synapse remote() file functionality. %s" % e.msg)

class RemoteProvider(AbstractRemoteProvider):
    def __init__(self, *args, **kwargs):
        super(RemoteProvider, self).__init__(*args, **kwargs)

        self._synapsec = synapseclient.Synapse(*args, **kwargs)
        self._synapsec.login(silent=True)


    def remote_interface(self):
        return self._synapsec

class RemoteObject(AbstractRemoteObject):
    """ This is a class to interact with the Synapse object store.
    """

    def __init__(self, *args, keep_local=False, provider=None, **kwargs):
        super(RemoteObject, self).__init__(*args, keep_local=keep_local, provider=provider, **kwargs)

        if provider:
            self._synapsec = provider.remote_interface()
        else:
            self._synapsec = synapseclient.Synapse(*args, **kwargs)
            self._synapsec.login(silent=True)

    # === Implementations of abstract class members ===

    def exists(self):
        try:
            ent = self._synapsec.get(self.remote_file(), downloadFile=False)
            return True
        except:
            return False

    def mtime(self):
        if self.exists():
            ent = self._synapsec.get(self.remote_file(), downloadFile=False)
            epochTime = ent.properties.modifiedOn
            return epochTime
        else:
            #raise synapseclient.client.SynapseFileNotFoundError("The file does not seem to exist remotely: %s" % self.remote_file())
            raise SynapseFileException("The file does not seem to exist remotely: %s" % self.remote_file())

    def size(self):
        if self.exists():
            ent = self._synapsec.get(self.remote_file(), downloadFile=False)
            fh_properties = syn._getFileHandle(ent.properties.dataFileHandleId)
            return int(fh_properties['contentSize'])
        else:
            return self._iofile.size_local

    def download(self, make_dest_dirs=True):
        if self.exists():
            # if the destination path does not exist, make it
            if make_dest_dirs:
                os.makedirs(os.path.dirname(self.file()), exist_ok=True)

            self._synapsec.get(self.file(), downloadLocation=os.path.dirname(self.file()))
        else:
            # raise synapseclient.client.SynapseFileNotFoundError("The file does not seem to exist remotely: %s" % self.remote_file())
            SynapseFileException("The file does not seem to exist remotely: %s" % self.remote_file())

    def remote_file(self):
        return "/"+self.file() if not self.file().startswith("/") else self.file()

    @property
    def name(self):
        return self.file()
