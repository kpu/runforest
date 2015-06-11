#!/usr/bin/env python3
import curses
import sys
import threading
import binascii

class Monitor:
  def init_from_manager(self, manager):
    pass
  def update(self):
    pass
  def add(self, task):
    self.update()
  def executing(self, task):
    self.update()
  def ready(self, task):
    self.update()
  def await_lock(self, task):
    self.update()
  def success(self, task, directory, was_cached):
    self.update()
  def failure(self, task, actual_command, ret):
    self.update()
  def close(self):
    pass
  def __enter__(self):
    return self
  def __exit__(self, type, value, traceback):
    pass

class SyncedPrint:
  """Print atomically from multiple threads."""
  def __init__(self, underlying = sys.stdout):
    self.underlying = underlying
    self.lock = threading.Lock()

  def print(self, *message):
    with self.lock:
      print("=============== ", *message, file = self.underlying)

class StderrMonitor(Monitor):
  def __init__(self, underlying = sys.stdout):
    self.synced_out = SyncedPrint(underlying)

  def await_lock(self, task):
    self.synced_out.print("LOCKED "+task.distinguished_name(' '))

  def executing(self, task):
    self.synced_out.print("RUNNING " + task.distinguished_name(' '))

  def success(self, task, directory, was_cached):
    self.synced_out.print(("CACHED " if was_cached else "SUCCESS ") + task.distinguished_name(' '))

  def failure(self, task, actual_command, ret):
    self.synced_out.print("FAILED " + task.distinguished_name(' ') + " with ", ret, "\n", actual_command)


class CursesMonitor(Monitor):
  def __init__(self):
    self.lock = threading.Lock()

  def __enter__(self):
    self.stdscr = curses.initscr()
    curses.noecho()
    self.stdscr.keypad(True)
    return self

  def __exit__(self, type, value, traceback):
    curses.nocbreak()
    self.stdscr.keypad(False)
    self.stdscr.clear()
    self.stdscr.refresh()
    curses.echo()
    curses.endwin()

  def init_from_manager(self, manager): 
    self.manager = manager
    self.status = [
      ["Failed", manager.failed],
      ["Running", manager.running],
      ["Queued", manager.queued],
      ["Depend", manager.waiting_derived],
      ["Locked", manager.waiting_lock]
    ]
    self.succeeded = 0
    self.cached = 0

  def draw_line(self, line, string):
    self.stdscr.addstr(line, 0, string)

  def dump_listing(self, status_entry, line):
    locked_set = status_entry[1]
    with locked_set.lock:
      if len(locked_set.inner) == 0:
        return line
      self.draw_line(line, status_entry[0])
      line += 1
      for r in locked_set.inner:
        content = binascii.hexlify(r.hash).decode('UTF-8')[0:6]
        content += ' ' + r.name
        if r.awaiting != 0:
          content += ' ' + str(r.awaiting)
        self.draw_line(line, content)
        line += 1
      line += 1
    return line

  def success(self, task, directory, was_cached):
    with self.lock:
      if was_cached:
        self.cached += 1
      else:
        self.succeeded += 1
    self.update()

  def update(self):
    with self.lock:
      summary = ", ".join([str(s[1].length()) + " " + s[0] for s in self.status])
      summary += ", " + str(self.succeeded) + " Succeeded" + ", " + str(self.cached) + " Cached"
      self.stdscr.clear()
      try:
        self.stdscr.addstr(0, 0, summary)
        line = 1
        for s in self.status:
          line = self.dump_listing(s, line)
      except curses.error:
        pass
      self.stdscr.refresh()
