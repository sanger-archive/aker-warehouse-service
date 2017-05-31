from nose.tools import *
import events_consumer

def setup():
  print "SETUP!"

def teardown():
  print "TEAR DOWN!"

def test_basic():
  print "I PASSED!"