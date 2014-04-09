import sys, os, signal

class threadWatcher:
	"""this class solves two problems with multithreaded
	programs in Python, (1) a signal might be delivered
	to any thread (which is just a malfeature) and (2) if
	the thread that gets the signal is waiting, the signal
	is ignored (which is a bug).

	The watcher is a concurrent process (not thread) that
	waits for a signal and the process that contains the
	threads.  See Appendix A of The Little Book of Semaphores.
	http://greenteapress.com/semaphores/

	I have only tested this on Linux.  I would expect it to
	work on the Macintosh and not work on Windows.
	"""

	def __init__(self):
		""" Creates a child thread, which returns.  The parent
			thread waits for a KeyboardInterrupt and then kills
			the child thread.
		"""
		self.child = os.fork()
		if self.child == 0:
			return
		else:
			print "#"*30
			print "main PID "+str(os.getpid()) \
				+"\n  |-- child PID "+str(self.child)
			print "#"*30
			self.watch()

	def watch(self):
		try:
			os.wait()
		except KeyboardInterrupt:
			# I put the capital B in KeyBoardInterrupt so I can
			# tell when the Watcher gets the SIGINT
			print 'KeyBoardInterrupt for multiThreading'
			self.kill()
		sys.exit()

	def kill(self):
		try:
			os.kill(self.child, signal.SIGKILL)
			print "killing PID "+str(self.child)+" ..."
		except OSError: pass

