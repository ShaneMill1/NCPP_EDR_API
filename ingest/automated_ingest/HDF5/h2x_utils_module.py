#!/opt/conda/envs/env/bin/python
# =================================================================
#
# Authors: Joseph Lang <joseph.lang@noaa.gov>
#
# Copyright (c) 2020 Joseph Lang - National Weather Service
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================



import copy;
import json;
import os;
import pprint as PP;
import sys;
import time;


if sys.version.startswith( "3." ):
   xrange = range
else:
   pass



# cml = command line
class command_line:

# here is the constructor method.
   def __init__( self, in_prefix_str = "-" ):

      self.set_prefix( in_prefix_str )
      self.__argv__ = copy.deepcopy( sys.argv )
      return None
# __init__()

# the destructor method.
   def __del__( self ):
      return None
# __del__()

   def get_prefix( self ):
      return self.__prefix__
# get_prefix()

   def set_prefix( self, new_prefix = "-" ):
# make sure that the specified "new prefix" is a string object.
      if not isinstance( new_prefix, str ): return self.get_prefix()
      self.__prefix__ = new_prefix
      return self.get_prefix()
# set_prefix()

# invoke this method to get a copy of the process's command line argument list (sys.argv).
   def argv( self ):
      return copy.deepcopy( self.__argv__ )
   # argv()

# invoke this method to get the length of the list of command line arguments for this process.
   def argc( self ):
      return len( self.argv() )
# argc()

   def is_cml_flag( self, flag_str ):
# initialize the flag for the search.
      flag_str = self.get_prefix() + flag_str

      max_i = self.argc()

      for i in range( max_i ):
         found = self.compare( flag_str, self.__argv__[i] ) == 0
         if not found: continue
         self.__found_index__ = i
         self.__value_index__ = self.__found_index__ + 1
         self.__was_last_argument__ = self.__value_index__ >= max_i
         return True
# end of for i in range( max_i ): loop.

      return False
   # is_cml_flag()

   def get_cml_arg( self, flag_str, default_str = None ):

# early out here...flag not on command line...or in the "list".
      if not self.is_cml_flag( flag_str ): return default_str

# ok, now the flag was found on the command line, but
# was there associated data to the right of the flag?
      if self.__was_last_argument__: return None

# the flag was found on the command line and there was
# an associated data value to the right of the flag.
      return self.__argv__[self.__value_index__]
   # get_cml_arg()

   def full_path( self, fn = None ):
      if not fn: fn = self.argvn( 0 )
      return os.path.realpath( fn )
# full_path()

   def argvn( self, n = 0 ):
# make sure that the specified "n" value is an int object.
      if not isinstance( n, int ): return None
# disallow negative n values.
      if n < 0: return None
# disallow n values greater than the last valid index for the argv list.
      last_index = self.argc() - 1
      if n > last_index: return None
      return self.argv()[n]
   # argvn()

   def compare( self, s1, s2 ):
      if s1 < s2: return -1
      if s1 == s2: return 0
      return 1
# compare()

# end of class command_line

def get_cwd():
   return os.getcwd()
# get_cwd()


def this_programs_name():
   this_program_fn = sys.argv[0]
   this_program_fn = full_path( this_program_fn )
   return this_program_fn
# this_programs_name()


def introduction( hdf5_fn, dataset_name ):
#
   this_program_fn = this_programs_name()
#
   print( \
   """Started Executing Program:
%s
At:
%s
Processing Dataset File:
%s
HDF5 Dataset Name:
%s
Current Directory:
%s
""" \
   %( \
    this_program_fn + new_line() + BN( this_program_fn ), \
   time.ctime( time.time() ), \
   full_path( hdf5_fn ) + new_line() + BN( hdf5_fn ), \
   dataset_name, \
   get_cwd(), \
   ) \
   )
#
   F()
#
   return None
#introduction()


def epilogue( script_start_time ):
#
   this_program_fn = this_programs_name()
#
   F()
   print( \
   """
Finished Executing Program:
%s
At:
%s
This script executed in %1.3f seconds.
""" \
   %( \
    this_program_fn + new_line() + BN( this_program_fn ), \
   time.ctime( time.time() ), \
   now() - script_start_time, \
   ) \
   )
   F()
#
   return None
#epilogue()


def F():
   sys.stdout.flush()
   sys.stderr.flush()
   return None
# F()


def cat_this_program():
   F()
   os.system( "cat " + this_programs_name() )
   F()
   return None
# cat_this_program()


def ls_hdf5_file( hdf5_fn ):
   F()
   print( "\nBegin ls output:" )
   os.system( \
   "ls -l --time-style=+'%%b %%d %%Y %%H:%%M:%%S' %s" \
   " | grep -iv total" \
   %( hdf5_fn ) )
   F()
   print( "End ls output:" )
   F()
   return None
# ls_hdf5_file()


def run_nc_dump( hdf5_fn ):
   F()
   print( "\nBegin ncdump output:" )
   F()
   os.system( "ncdump " + hdf5_fn )
   F()
   print( "End ncdump output:" )
   F()
   return None
# run_nc_dump()


def get_joes_variables():
   hdf5_fn = full_path( "../example_dataset.hdf5" )
   dataset_name = "jl_first_dataset"
   shape_tuple = ( 4, 6 )
   data_dimensions_tuple = ( 2, )
   return hdf5_fn, dataset_name, shape_tuple, data_dimensions_tuple
# get_joes_variables()


def object_has_attribute( the_object, attribute_name_str ):
   xl = dir( the_object )
   return attribute_name_str in xl
# object_has_attribute()



def dims_list_from_tuple( input_tuple ):
# rv_l = return value l ist.
   rv_l = list()
# process the input tuple.
# tev = tuple entry value.
   for tev in input_tuple:
     rv_l.append( tev ) # store the tuple entry in the return value list.
# end of for tev in input_tuple: loop.
#
# return the list to the caller.
   return rv_l
# dims_list_from_tuple()



def python_version():
   return sys.version
# python_version()


def python2():
   return python_version().startswith( "2" )
# python2()


def python3():
   return python_version().startswith( "3" )
# python3()

def empty_str():
   return str()
# empty_str()


def get_callers_function_name( the_name_index = 2 ):

   the_callers_function_name = sys._getframe(the_name_index).f_code.co_name

   return the_callers_function_name
# get_callers_function_name()


def get_function_name():

   the_function_name = sys._getframe(1).f_code.co_name

   return the_function_name
# get_function_name()


def lineno():
   """
This code provides a lineno() function to make it easy to grab the line
number that we're on.
Danny Yoo (dyoo@hkn.eecs.berkeley.edu)
Returns the current line number in our program.
   """
   return inspect.currentframe().f_back.f_lineno
# lineno()



def tab():
   return "\t"
# tab()



def space():
   return " "
# space()


def print_this( s, print_to_file = None ):
   if print_to_file == None: print_to_file = sys.stdout
   print_to_file.write( s )
   return None
# print_this()


def new_line():
   return os.linesep
# new_line()



def BN( s = get_cwd() ):
   return os.path.basename( s )
# BN()


def DN( s = get_cwd() ):
   return os.path.dirname( s )
# DN()


def real_path( fn = sys.argv[0] ):
   return os.path.realpath( fn )
# real_path()


def full_path( fn = sys.argv[0] ):
   return real_path( fn )
# full_path()


def CD( to_dir ):
   os.chdir( to_dir )
   return get_cwd()
# CD()


def initialize_argument_dictionary( additional_initialization_function = None ):

# ad = argument dictionary (ad)
# a dictionary that will buffer many of the variables
# used throughout the calling process;
# the variables include
# the command line arguments and
# configuration data and accumulated data variables/values
# that are utilized by the functions within the calling script.
   ad = dict()

# process start time.
   ad["start time"] = now()

   ad["start directory"] = get_cwd()

# command line stuff.
   sys.argv[0] = full_path( sys.argv[0] )

   ad["av"] = ad["argv"] = sys.argv

   ad["ac"] = ad["argc"] = len( ad["argv"] )

# get an object for processing the command line.
   ad["command line processing object"] = \
   ad["cml o"] = command_line()

# anything else for initializations?
   if  additional_initialization_function == None:
      pass
   else:
      ad = additional_initialization_function( ad )

# return the initialized argument dictionary (ad) to the caller.
   return ad
# initialize_argument_dictionary()


def now():
   return time.time()
# now()



# begin command line argument processing functions.

cml_arg_flag_prefix_str = "-"
cml_flag_found = False

def get_cml_arg_flag_prefix_str():

   global cml_arg_flag_prefix_str;

def get_cml_arg( cml_flag, default_str = empty_str() ):

   if is_cml_arg( cml_flag ):
      cml_str = process_cml_args( cml_flag, default_str )
   else:
      cml_str = default_str

   return cml_str
# get_cml_arg()


def is_cml_arg( cml_flag ):

   global cml_flag_found;

   process_cml_args( cml_flag, None )

   return cml_flag_found
# is_cml_arg()


def process_cml_args( cml_flag, default_str ):

   global cml_flag_found;

   cml_flag = get_cml_arg_flag_prefix_str() + cml_flag

   the_return_cml_str = default_str

   cml_flag_found = False

# refer to the command line.
   av = sys.argv

   ac = len( av )

   for i in xrange( 1, ac ):
      cml_flag_found = av[i] == cml_flag
      if not cml_flag_found: continue
# don't try to access data beyond the end of the command line argument list.
      j = i + 1
      if j < ac: the_return_cml_str = av[j]
      break
# end of for i in xrange( ac ): loop.

   return the_return_cml_str
# process_cml_args()


def set_cml_arg_flag_prefix_str( new_value_str ):

   global cml_arg_flag_prefix_str;

# some error checking.
   if ( isinstance( new_value_str, str ) ) and \
   ( new_value_str != None ) and \
   ( len( new_value_str ) > 0 ) and \
   ( new_value_str.count( " " ) == 0 ):
      cml_arg_flag_prefix_str = new_value_str
   else:
      pass

   return get_cml_arg_flag_prefix_str()
# set_cml_arg_flag_prefix_str()
# end command line argument processing functions.


def get_cml_arg_flag_prefix_str():

   global cml_arg_flag_prefix_str;

   return cml_arg_flag_prefix_str
# get_cml_arg_flag_prefix_str()


def E():
   id_str = "Enter"
   print_this( "%s %s()" %(id_str,get_callers_function_name()) + new_line() )
   return None
# E()


def L():
   id_str = "Leave"
   print_this( "%s %s()" %(id_str,get_callers_function_name()) + new_line() )
   return None
# L()


def pretty_print( the_data, output_stream = sys.stdout ):

   pp = PP.PrettyPrinter( \
   indent = 3,
   width = 80,
   depth = None,
   stream = output_stream \
   )

   pp.pprint( the_data )

   return None
# pretty_print()



def get_command_line_argument_info_string():

   nl = new_line()

   argv0 = sys.argv[0]

   args = "Begin Command Line Arguments:" + nl + argv0 + nl + nl.join( sys.argv[1:] )

   args = args.replace( nl, ")" + nl + "(" )

   args = args.replace( ":)", ":", 1 )

   args += ")" + nl + "End Command Line Arguments."

   return args
# get_command_line_argument_info_string()


# end of command line argument processing code.


def the_type( some_object = None ):
   return str( type( some_object ) )
# the_type()


def this_type( this_object, this_id_str ):
   this_type_str = the_type( this_object )
   return this_id_str in this_type_str
# this_type()

def is_a_string( test_object = None ):
   return this_type( test_object, "'str'>" )
# is_a_string()


def JSP( object_to_print, info_str, the_indent = 4 ):
   E()
   print_this( """Called by:
%s()
begin %s""" \
   %( get_callers_function_name(), info_str ) )
#   print_str = json.dumps( object_to_print, indent = the_indent )
#   J.print_this( print_str )
   pretty_print( object_to_print )
   print_this( \
   """
end %s
""" \
   %( info_str ) )
   L()
   return None
# JSP()
