from io import TextIOWrapper
import sys
import subprocess as sp
from threading import Thread

class PipeWriter:
	def __init__(self, towrite):
		self._towrite = towrite
		
	def write(self, line):
		self._towrite.write(line)

class ListWriter(PipeWriter):
	def write(self, line):
		self._towrite.append(line[:-1].decode("utf-8"))
		print(line)

class TextIOWriter(PipeWriter):
	def write(self, line):
		self._towrite.write(line.decode("utf-8"))
		
class shell(sp.Popen):
	_processes = []
	def __init__(self, cmd):
		super(shell, self).__init__(cmd, shell=True, stdin = sp.PIPE, stdout=sp.PIPE, close_fds=True)
		self._stdout_free = True
		self._pipethread = None
		shell._processes.append(self)
	
	def wait(self):
		super(shell, self).wait()
		if self._pipethread:
			self._pipethread.join()
		
	@staticmethod
	def join():
		for p in shell._processes:
			if p._stdout_free:
				for l in p._stdoutlines(): pass
			p.wait()
		shell._processes = []
		
	def _stdoutlines(self):
		while True:
			o = self.stdout.readline()
			if not o:
				return
			yield o

	def _write_pipes(self, pipes, toclose):
		for l in self._stdoutlines():
			for pipe in pipes:
				pipe.write(l)
		for pipe in toclose:
			pipe.close()
				
	def __or__(self, other):
		if not isinstance(other, tuple):
			other = tuple([other])

		pipes = []
		toclose = []
		for o in other:
			writer = None
			if isinstance(o, shell):
				writer = PipeWriter(o.stdin)
				toclose.append(o.stdin)
				# move all stdin to the beginning of the pipe
				o.stdin = self.stdin
			elif isinstance(o, TextIOWrapper):
				writer = TextIOWriter(o)
			elif isinstance(o, list):
				writer = ListWriter(o)
			else:
				raise ValueError("Only shell, files, stdout or lists allowed right to a shell pipe.")
			pipes.append(writer)
		
		self._stdout_free = False
		self._pipethread = Thread(target = self._write_pipes, args = (pipes, toclose)).start()
			
		if len(other) == 1:
			return other[0]
	

shell("echo b; echo a; echo c > foo")
shell("echo b; echo a; echo c") | shell("sort") | sys.stdout

shell("echo a; echo b") | (shell("sort"), shell("cut -f1"))

x = shell("echo 2; echo 1")
x | shell("sort") | sys.stdout
shell.join() # ensure that all shells are finished before next line
print("test")

y = []
shell("echo foo; echo ''; echo bar") | y
shell.join() # ensure that all shells are finished before next line
import time; time.sleep(5)
print(y)

shell.join()