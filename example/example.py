#!/usr/bin/env python3
import os, sys
#Get directory where this script lives
directory = os.path.dirname(os.path.realpath(__file__))
#Import experiment from the parent directory
sys.path.insert(0, os.path.join(directory, '..'))
from runforest import *

#Setup some file locations.  These are just for convenience in the examples.
#They are not used by the framework.
TEST_FILE = os.path.join(directory, "test_file")
WORK_DIR = os.path.join(directory, "work")

def input_check():
  #The Input class specifies a file used as input to the pipeline.
  #Files less than 1 MB use a content-based hash, as demonstrated here.
  #Files larger than 1 MB are hashed based on inode, mtime, and size.
  before_touch = Input(TEST_FILE).hash
  #touch the file so the modification time changes
  open(TEST_FILE, "a").close()
  after_touch = Input(TEST_FILE).hash
  assert before_touch == after_touch

def sort_uniq():
  #Create a task that will run "sort input".  The return value is a future that
  #represents the stdout of the command.
  sort = stdout("sort", Input(TEST_FILE), name="sort")
  #Run uniq on the output of sort.  This creates a dependency.
  unique = stdout("uniq -c", sort, name="unique")
  #Execute tasks in the given working directory with up to 2 threads, reaching
  #the target given by unique.  Wait for completion and return true on success.
  if run(WORK_DIR, 2, unique).wait():
    print("Output is in", unique.path)

def multiple_outputs():
  #This demonstrates multiple outputs.  The Output class is a placeholder for
  #a file name to be written.  The Outputs are transformed into a future (a
  #Derived class) and returned in the same order as they were specified.
  (sort, unique) = step("sort", Input(TEST_FILE), "|tee", Output("sorted"), "|uniq -c >", Output("unique"), name="multiple_outputs")
  run(WORK_DIR, 1, unique).wait()

def relevance():
  #Typically the command line arguments are all part of the hash to determine
  #if a task needs to be rerun.  But some arguments do not impact output.  For
  #example, sort's memory settings do not change the output.  Such arguments 
  #can be wrapped with Irrelevant.
  sort1 = stdout("sort", Irrelevant("-S 1M"), Input(TEST_FILE), name="sort1")
  sort2 = stdout("sort", Irrelevant("-S 2M"), Input(TEST_FILE), name="sort2")
  assert sort1.hash == sort2.hash
  #Tasks that have the same hash are deduplicated.  In this case, only sort1
  #will run and its output will be given to both uniq and uniq_c
  uniq = stdout("uniq", sort1, name="uniq")
  uniq_c = stdout("uniq -c", sort2, name="uniq_c")
  run(WORK_DIR, 1, uniq, uniq_c).wait()

def locking():
  #Each task directory has a lock.  So even if you run the same task in
  #parallel with completely separate processes, they will not corrupt each
  #other.
  runner1 = run(WORK_DIR, 1, stdout("uniq", Input(TEST_FILE), name="runner1"))
  runner2 = run(WORK_DIR, 1, stdout("uniq", Input(TEST_FILE), name="runner2"))
  assert runner1.wait()
  assert runner2.wait()

if __name__ == "__main__":
  input_check()
  sort_uniq()
  multiple_outputs()
  relevance()
  locking()
