#!/usr/bin/env python3
import hashlib
import os
import struct
import binascii
import queue
import threading
import subprocess
import sys
import shutil
import fcntl
import errno
import monitor

class UnimplementedHash(Exception):
  """Exception thrown if something was provided that we didn't know how to hash."""
  def __init__(self, item):
    self.item = item

  def str(self):
    "Could not hash " + repr(self.item)

class Hasher:
  """Python's own hash is weak i.e. hash(2) == 2.  This uses sha256 and provides the necessary conversions to raw bytes."""
  def __init__(self):
    self.internal = hashlib.sha256();

  def add(self, value):
    """Accumulate a value in the hash."""
    try:
      if value.hash is None:
        return self
    except AttributeError:
      pass
    self.internal.update(self.encode(value))
    return self

  def encode(self, value):
    """Encode an object as bytes for hashing.  Works for anything with .hash, str, integer, and floats."""
    try:
      return value.hash
    except AttributeError:
      pass
    try:
      return value.encode('UTF-8')
    except AttributeError:
      pass
    try:
      return struct.pack('q', value)
    except struct.error:
      pass
    try:
      return struct.pack('d', value)
    except struct.error:
      pass
    raise UnimplementedHash(value)

  def result(self):
    return self.internal.digest()

def hash_args(*args):
  """Hash all the arguments together."""
  hasher = Hasher()
  for i in args:
    hasher.add(i)
  return hasher.result()
     
class Argument:
  """Base class for arguments to a Task.  Arguments can do anything they want
     to a Task in the act(task) method.  Typically, they append themselves to
     the task's list of command-line arguments.  This is what the base class
     does."""
  def act(self, task):
    """Some arguments need to tell the Task to e.g. depend on a file or make an output.  The Task constructor calls argument.act(self) on each argument, giving it the opportunity to inform the task."""
    pass

  def visit(self, host):
    host.visit(self)

class ConstantArgument(Argument):
  """Command-line argument that's just a constant string.  If Task is called
    with an argument that does not respond to act(task), then it is presumed
    to be a constant argument and wrapped appropriately."""
  def __init__(self, value):
    self.value = value.__str__()
    self.hash = Hasher().add(value).result()

  def command(self, directory):
    return self.value

class Input(Argument):
  """Argument for a file that exists at the beginning.  The hash is based on
     inode, modification time, and size."""
  def __init__(self, name):
    stat = os.stat(name)
    if stat.st_size < 1048576:
      """Content-based hash for small files."""
      h = hashlib.sha256()
      with open(name, 'rb') as opened:
        #TODO: stream this
        h.update(opened.read(stat.st_size))
      self.hash = h.digest()
    else:
      """Metadata hash for large files."""
      self.hash = hash_args(stat.st_ino, stat.st_mtime, stat.st_size)
    self.path = os.path.abspath(name)
    self.name = name

  def command(self, directory):
    return self.path

class Output(Argument):
  """Specify an output that appears as a named file in the command line.  These will be converted to Derived files and returned in the same order."""
  def __init__(self, name = "out"):
    if name.find('/') == 0 and name != "/dev/null":
      print("Warning: output " + name + " contains a /.", file=sys.stderr)
    self.hash = None
    self.name = name

  def act(self, task):
    task.register_output(self.name)
 
  def command(self, directory):
    if os.path.isabs(self.name):
      return self.name
    else:
      return os.path.join(directory, self.name)

class Derived(Argument):
  """A file that comes from the output of a task.  These live in task.outputs and are returned by e.g. run()"""
  def __init__(self, task, name, hash_code):
    self.task = task
    self.name = name
    self.waiting = []
    self.hash = hash_code
  
  def act(self, task):
    """When a task uses this file as input: tell the task it's waiting for this file and record the task so it can be notified later."""
    task.register_input(self)
    self.waiting.append(task)

  def generated(self, manager, directory):
    """Notify the derived file that it has been generated in the given directory.  This tells all the waiting tasks that their input is ready."""
    self.path = os.path.join(directory, self.name)
    for dependent in self.waiting:
      dependent.input_ready(manager)

  def command(self, directory):
    return self.path

  def as_task(self):
    """Targets can be specified as Task or Derived (in which case the underlying task is desired)"""
    return self.task

class InDirectory(Argument):
  """A file in the working directory, but which is not an input or output, i.e. for temporary usage"""
  def __init__(self, name):
    self.name = name
    self.hash = None

  def command(self, directory):
    return os.path.join(directory, self.name)

class ArgumentList(Argument):
  """A list of arguments.  This just delegates to the arguments provided, hashing them together and joining them with spaces."""
  def __init__(self, *args):
    self.args = []
    self.interpret_item(args)
    self.hash = hash_args(*self.args)

  def interpret_item(self, item):
    """Users configure tasks with a sequence of Argument classes, lists, tuples, and constants like strings and numbers.  This method interprets the type and wraps it appropriately so that everything is an Argument."""
    if hasattr(item, 'act'):
      self.args.append(item)
      return
    #Flatten lists and tuples.
    elif type(item) == type([]) or type(item) == type(()):
      for i in item:
        self.interpret_item(i)
    else:
      self.args.append(ConstantArgument(item))
 
  def act(self, task):
    for arg in self.args:
      arg.act(task)

  def command(self, directory):
    commands = [item.command(directory) for item in self.args]
    commands = [c for c in commands if c is not None]
    return ' '.join(commands)

  def visit(self, host):
    for arg in self.args:
      arg.visit(host)

class NoSpace(ArgumentList):
  """Join arguments without putting a space in between.  Useful for e.g. rsync hostname:file"""
  def command(self, directory):
    commands = [item.command(directory) for item in self.args]
    commands = [c for c in commands if c is not None]
    return ''.join(commands)

class Defer(Argument):
  """Wrapper around another Argument.  Useless on its own, but overridden in classes that inherit."""
  def __init__(self, *args):
    self.inner = ArgumentList(args)
    self.hash = self.inner.hash

  def act(self, task):
    self.inner.act(task)

  def command(self, directory):
    return self.inner.command(directory)

  def visit(self, host):
    self.inner.visit(host)

class Irrelevant(Defer):
  """Designate a portion of the arguments as irrelevant to the hash.  For example, memory usage settings and temporary file location."""
  def __init__(self, *args):
    self.inner = ArgumentList(args)
    self.hash = None

class Implicit(Defer):
  """Designate a portion of the arguments as implicit i.e. output generated but not seen."""
  def command(self, directory):
    return None

class Task:
  """A command to run, with input and output files.  A Task is constructed with
     a list of command-line arguments.  There are several types of arguments:

     strings, numbers, floats, etc. are taken as constants.  Formally, anything
     that does not respond to act() is interpreted as a constant (and must
     respond to __string__)

     Input is a file that already exists, like a corpus.  

     Output is a placeholder for a file to be created.  Each Output object
     passed in is promoted to a Derived object and placed in task.outputs.
     The convenience function step(*args) constructs a task and returns 

     Derived is a file to be created.  A Task provides task.outputs with its
     derived files.  Derived files are provided just like any other argument.
     
     Task's constructor calls argument.act(self) for each argument.  Arguments
     are responsible for calling:
        append to add to the command line and to the hash
        register_input to indicate that 
     The arguments are hashed in order to form the
     task's hash (except any wrapped with Irrelevant)."""

  def __init__(self, name, *arguments):
    self.args = ArgumentList(arguments)
    self.inputs = []
    self.outputs = []
    self.awaiting = 0
    self.name = name
    self.hash = self.args.hash
    self.args.act(self)
    self.ran_in = None
    self.ran_lock = threading.Lock()

  def __hash__(self):
    """For use in a hash table."""
    return struct.unpack('q', self.hash[0:8])[0]

  def __eq__(self, other):
    """Tasks with the same hash are presumed to be the same."""
    return self.hash == other.hash

  def __cmp__(self):
    return self.hash

  def register_output(self, name):
    """An Argument might result in an output file.  Arguments can call this to indicate that they produce such a file, typically in their act function."""
    self.outputs.append(Derived(self, name, hash_args(self, len(self.outputs))))

  def register_input(self, derived):
    """Indicate that this Task depends on a Derived file.  Typically this is called by Derived.act"""
    self.inputs.append(derived)
    self.awaiting += 1

  def input_ready(self, manager):
    """Inform the Task that one of the inputs is ready.  This is called by Derived after it has been informed that it was generated."""
    with self.ran_lock:
      self.awaiting -= 1
      ready = (self.awaiting == 0)
    if ready:
      manager.ready(self)

  def directory(self):
    """Subdirectory where the task will run, excluding the working directory."""
    return self.name

  def command(self, base):
    """Shell command to run for this task.  base is the base working directory."""
    outdir = os.path.join(base, self.directory())
    return self.args.command(outdir)

  def ran(self, manager, directory):
    """Notify the task that it ran, causing the Derived outputs to be notified
       that they are ready."""
    with self.ran_lock:
      self.ran_in = directory
      #Copy this reference so subsume_duplicate doen't edit this object below.
      existing_outputs = self.outputs
    for output in existing_outputs:
      output.generated(manager, directory)

  def subsume_duplicate(self, other, manager):
    """Merge a duplicate task into this one, taking over its downstream responisbilities."""
    for out in other.outputs:
      out.task = self
    with self.ran_lock:
      ran = (self.ran_in is not None)
      self.outputs = self.outputs + other.outputs
    if ran:
      """Already ran.  Let the downstream know."""
      for out in other.outputs:
        out.generated(manager, self.ran_in)

  def as_task(self):
    """Targets can be specified as Task or Derived (in which case the underlying task is desired)"""
    return self

  def distinguished_name(self, delim = ''):
    """name plus some of the hash for use like a git revision."""
    code = binascii.hexlify(self.hash).decode('UTF-8')[0:6]
    return self.name + delim + code

class LockedSet:
  """Wrapper around a set() with a lock and atomic move."""
  def __init__(self):
    self.inner = set()
    self.lock = threading.Lock()

  def add(self, element):
    """Add element and return true if the insertion was successful."""
    with self.lock:
      prelength = len(self.inner)
      self.inner.add(element)
      return prelength != len(self.inner)

  def move(self, item, to):
    """Atomically move item from self to to"""
    if id(self) < id(to):
      first = self
      second = to
    elif id(self) > id(to):
      first = to
      second = self
    else:
      return
    with first.lock:
      with second.lock:
        self.inner.remove(item)
        to.inner.add(item)

  def length(self):
    with self.lock:
      return len(self.inner)

class TaskManager:
  """Keep track of task status but don't actually run them (that's LocalRunner's job).  Methods are called to change job status, with the intent that this could produce a nice monitor."""
  def __init__(self, monitor):
    """Start a TaskManager so that jobs can be added to it."""

    """A task belongs to exactly one of the LockedSets below."""
    """Finished running successfully (or was cached)."""
    self.complete = LockedSet()
    """Ran and resulted in failure."""
    self.failed = LockedSet()
    """Waiting for a Derived file to be produced by another task."""
    self.waiting_derived = LockedSet()
    """Tried to run the task but failed to acquire a lock.  A thread is waiting for the lock to be released, at which point the task will move to queued."""
    self.waiting_lock = LockedSet()
    """Ready to run, waiting for a free CPU."""
    self.queued = LockedSet()
    """Currently running."""
    self.running = LockedSet()

    """Producer-consumer queue of tasks to run.  TaskManager produces to this queue and LocalRunner consumes from this queue.  Tasks that go into this queue are only marked as done if they transition to complete or failed status.  A task that attempts to run but encounters a lock is not marked as done but moved to waiting_lock status.  Therefore one can wait on completion of the queue items to wait for completion for all tasks."""
    self.run_queue = queue.Queue()

    """Dictionary from task.hash to task.  All tasks known to the TaskManager appear in this dictionary.  It is used to deduplicate tasks."""
    self.all_tasks = {}
    self.all_tasks_lock = threading.Lock()

    self.monitor = monitor
    self.monitor.init_from_manager(self)

  def add(self, target):
    target = target.as_task()
    dupe = False
    with self.all_tasks_lock:
      if target.hash in self.all_tasks:
        already = self.all_tasks[target.hash]
        if target is already:
          return
        dupe = True
      else:
        self.all_tasks[target.hash] = target
    if dupe:
      #Sumsume without lock.
      already.subsume_duplicate(target, self)
      return
    #Task is not a duplicate.  Add it to the appropriate queue.
    if target.awaiting == 0:
      self.queued.add(target)
      self.run_queue.put(target)
    else:
      self.waiting_derived.add(target)
    self.monitor.add(target)
    for i in target.inputs:
      self.add(i.task)

  def executing(self, task):
    self.queued.move(task, self.running)
    self.monitor.executing(task)

  def ready(self, task):
    with self.all_tasks_lock:
      #Might be ready but not necessary to reach the target we want.
      move = (task.hash in self.all_tasks and id(self.all_tasks[task.hash]) == id(task))
    if move:
      self.waiting_derived.move(task, self.queued)
      self.run_queue.put(task)
      self.monitor.ready(task)

  def await_lock(self, task, lock_file):
    """Failed to acquire a lock, move to lock purgatory."""
    self.running.move(task, self.waiting_lock)
    threading.Thread(target = TaskManager.lock_waiter, args = [self, task, lock_file]).start()
    self.monitor.await_lock(task)
    #Note: no task_done().  This still counts as as running task in run_queue.

  def lock_waiter(self, task, lock_file):
    """Wait to acquire the lock then release it.  When the task makes it to the run stage, we'll try to acquire the lock again.  This avoids holding the lock while the task is waiting to run (so maybe some other machine/process can run it)."""
    fcntl.lockf(lock_file, fcntl.LOCK_EX)
    fcntl.lockf(lock_file, fcntl.LOCK_UN)
    lock_file.close()
    self.waiting_lock.move(task, self.queued)
    self.run_queue.put(task)
    #The task was already in the run_queue and never marked as done.
    self.run_queue.task_done()

  def success(self, task, directory, was_cached):
    task.ran(self, directory)
    self.running.move(task, self.complete)
    self.monitor.success(task, directory, was_cached)
    self.run_queue.task_done()

  def failure(self, task, actual_command, ret):
    self.running.move(task, self.failed)
    self.monitor.failure(task, actual_command, ret)
    self.run_queue.task_done()

  def wait(self):
    """Wait for all tasks to complete.  Return true if all tasks succeeded."""
    self.run_queue.join()
    self.monitor.close()
    with self.all_tasks_lock:
      with self.complete.lock:
        return len(self.complete.inner) == len(self.all_tasks)

def run_worker(manager, runner):
  #Run as a daemon thread.
  while True:
    task = manager.run_queue.get()
    manager.executing(task)
    try:
      runner.run(task, manager)
    except Exception as e:
      manager.failure(task)
      raise e

class LocalRunner:
  """After tasks have been configured, this knows how to run them.  Configure
     with a root path to store tasks and the number of parallel tasks to run."""
  def __init__(self, path, threads, manager):
    self.base = path
    try:
      os.mkdir(path)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise e
    for i in range(threads):
      thread = threading.Thread(target = run_worker, args=[manager, self])
      thread.daemon = True
      thread.start()

  def run(self, task, manager):
    directory = os.path.join(self.base, task.directory())
    command = task.command(self.base)
    if os.path.isfile(directory + "/success"):
      manager.success(task, directory, True)
      return

    try:
      os.mkdir(directory)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise e      
  
    lock_file = open(os.path.join(directory, "lock"), "wb")
    try:
      fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as e:
      if e.errno != errno.EACCES and e.errno != errno.EAGAIN:
        raise e
      manager.await_lock(task, lock_file)
      return

    try:
      #Now that we have the lock, check again for success.
      if os.path.isfile(directory + "/success"):
        manager.success(task, directory, True)
      else:
        self.run_lock_held(task, manager, directory, command)
    finally:
      fcntl.lockf(lock_file, fcntl.LOCK_UN)
      lock_file.close()


  DEVNULL = open(os.devnull, 'r+')
  def run_lock_held(self, task, manager, directory, command):
    actual_command = "set -e -o pipefail +o posix\n" + command
    command_file = open(directory + '/ran.sh', 'w')
    command_file.write(actual_command + "\n")
    command_file.close()
    ret = subprocess.call(actual_command, shell=True, stdin=LocalRunner.DEVNULL, stderr=open(directory + "/stderr", 'w'))
    if ret != 0:
      manager.failure(task, actual_command, ret)
      return
    open(directory + '/success', 'w').close()
    manager.success(task, directory, False)

class MissingTaskName(Exception):
  """Thrown if a task is given without a name."""
  pass

def step(name, *args):
  """Convenience function: construct a task and return its output.
     For example:
       tokenized = step("tokenizer.perl -l en <", text, ">", Output())
     Throws an exception if the task does not have exactly one output.
  """
  return Task(name, *args).outputs

class NotOneOutput(Exception):
  """Thrown if one calls step with a different number of outputs than 1.
     If you need multiple outputs, use 
       step("command", Output(), Output()).outputs
  """
  pass

def stdout(name, *args):
  """Run a task and return its stdout."""
  to_star = args + (">", Output())
  outputs = step(name, *to_star)
  if len(outputs) != 1:
    raise NotOneOutput()
  return outputs[0]

def run(path, threads, *tasks, monitor = monitor.StderrMonitor()):
  """In the specified working directory with the given number of threads, run tasks.  This does not block; instead call .wait() on the return value (which is a TaskManager)."""
  manager = TaskManager(monitor)

  #This spawns off threads so it does't go out of scope.
  LocalRunner(path, threads, manager)

  for task in tasks:
    manager.add(task)
  
  return manager

class FlattenVisitor:
  '''Visitor to task arguments that just assembles a flat array'''
  def __init__(self, args):
    self.out = []
    args.visit(self)

  def visit(self, arg):
    self.out.append(arg)

def shorthand_trace(task_or_derived):
  task = task_or_derived.as_task()
  trace = []
  for arg in FlattenVisitor(task.args).out:
    if type(arg) is Input:
      trace.append(arg.name)
    elif type(arg) is Derived:
      trace.append(shorthand_trace(arg))
  return task.distinguished_name() + '(' + ', '.join(trace) + ')'

