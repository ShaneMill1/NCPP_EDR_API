import copy;
import h5py;
import inspect;
import json;
import os;
import pprint as PP;
import io;
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


def get_object_dir_list( the_object ):
   return dir( the_object )
# get_object_dir_list()

def get_object_dir_str( o ):
   return new_line().join( get_object_dir_list( o ) )
# get_object_dir_str()


def object_has_attribute( the_object, attribute_name_str ):
# learned of this built-in function on 06/22/2020, python does this work already!
   return hasattr( the_object, attribute_name_str )
# old code below.
   xl = get_object_dir_list( the_object )
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
   av = copy.deepcopy( sys.argv )
   ac = len( av )
   nl = new_line()

   rs = "Begin Command Line Arguments:" + nl

   for i in range(  ac ):
      rs = rs + "(" + av[i] + ")" + nl
#      print("zz i: %d (%s)" %( i,av[i]))
# end of for i in range( ac ): loop.

   rs += "End Command Line Arguments:" + nl

   return rs

# old code below.
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
   return this_type( test_object, "<class 'str'>" )
# is_a_string()


def JSP( object_to_print, info_str,  the_indent = 4 ):
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


# added by joseph.lang@noaa.gov to support
# Shane's findings (shane.mill@noaa.gov)
# that xarr needs a str object not a bytes object.
#
def bytes_object_to_str_object( test_object ):
#
   if is_a_bytes_object( test_object ):
      print("ding")
      return test_object.decode( 'ascii' )
#jl#      str_object = str( test_object )
#jl#      if str_object.startswith( "b'" ): str_object = str_object[2:] # this will chop off the first 2 bytes the (b').
#jl#      if str_object.endswith( "'" ): str_object = str_object[:-1] # this will chop off the trailing ("'").
#jl#      return str_object
   else:
      return test_object
# bytes_object_to_str_object()


def is_a_bytes_object( test_object = None ):
   return this_type( test_object, "<class 'bytes'>" )
# is_a_bytes_object()


def is_a_numpy_object( test_object = None ):
   return this_type( test_object, "<class 'numpy" )
# is_a_numpy_object()

def store_value_in_dictionary( the_dict, the_key, the_value ):
   the_dict[the_key] = the_value
   return the_dict[the_key]
# store_value_in_dictionary()


def open_hdf5_file( hdf5_filename, open_mode = 'r' ):
# try to open the .HDF5 file,
# get an h5py File object to use to that is get a hdf5 file object,
# used toreference the file's contents.
# remember that the h5py.File object "acts" like a dictionary.
   hdf5_file_object = \
   h5py.File( hdf5_filename, open_mode )

# return the hdf5 file object to the caller.
   return hdf5_file_object
# open_hdf5_file()


def produce_hdf5_file_report( the_hdf5_filename, the_hdf5_group = "/" ):

   def process_hdf5_tree( the_hdf5_object, the_data_separator = tab() ):
      """
    Iterate through groups in a HDF5 file and prints the groups and datasets names and datasets attributes
"""
      if type( the_hdf5_object ) in [ h5py._hl.group.Group, h5py._hl.files.File ]:
#
         for key in the_hdf5_object.keys():
            print( the_data_separator, '-', key, ':', the_hdf5_object[key] )
            process_hdf5_tree( the_hdf5_object[key], the_data_separator = the_data_separator + tab() )
         # end of for key in the_hdf5_object.keys(): loop.
#
      elif type( the_hdf5_object ) == h5py._hl.dataset.Dataset:
#
         for key in the_hdf5_object.attrs.keys():
            print( the_data_separator + tab(), '-', key, ':', the_hdf5_object.attrs[key] )
# end of         for key in the_hdf5_object.attrs.keys(): loop.
#

      return None
# process_hdf5_tree()
# start/begin  produce_hdf5_file_report()
   the_hdf5_f = open_hdf5_file( the_hdf5_filename )
   process_hdf5_tree( the_hdf5_f[the_hdf5_group] )
   return None
# produce_hdf5_file_report()


def is_hdf5_file_object( o ):
   return type( o ) == h5py._hl.files.File
# is_hdf5_file_object()


def is_a_group_object( o ):
   return type( o ) == h5py._hl.group.Group
# is_a_group_object()


def is_a_dataset_object( o ):
   return type( o ) == h5py._hl.dataset.Dataset
# is_a_dataset_object()


def search_and_replace( s, s_str, r_str ):
   """
this function searches the string (s),
for the string (s_str (search string)),
and replaces all occurrences of the search string
with the value of the replace string (r_str).
   """
   while s.find( s_str ) >= 0: s = s.replace( s_str, r_str )
   return s
# search_and_replace()


def skip_from_dict():
   return is_cml_arg( "skip_from_dict" )
# skip_from_dict()


def use_print_dict():
   return is_cml_arg( "print_dict" )
# use_print_dict()


def print_dict( the_dict, label_str ):
# should we be here?
   if not use_print_dict(): return None
   stop_code_ccomment = """
# new function code as of 07/09/2020, get formatting provided by json module.
# Thanks to Shane Mill.
   E()
   if len( label_str ) > 0: print( "Begin: " + label_str )
#convert the dictionary to json so we can look at
# the formatted dictionary in a more readable way
   json_output = json.dumps( the_dict, indent = 4 )

   print( json_output )
#
   if len( label_str ) > 0: print( "End: " + label_str )
   L()
   return None
end of stop code comment."""

# here I write to a "string buffer" so I can get the functionality of pretty_print()
# without sending the data to a disk file first.
# sf = "string file",
# actually a io.StringIO object that "looks" and acts like a file object.
   E()
   if len( label_str ) > 0: print( "Begin: " + label_str )
   sf = io.StringIO()
   pretty_print( the_dict, sf )
# now reference the buffer that pretty_print() used for its output.
   s = sf.getvalue()
# now edit the buffer for easier viewing.
   """comments
   nl = new_line()
   lbc = "{"
   rbc = "}"
   lbk = "["
   rbk = "]"
   comma = ","
   semi = ';'
   empty_str = str()
# let the editing begin.
# ss = search string.
   for ss in [ lbc, rbc, lbk, rbk ]:
# rs = replace string.
      rs = nl + ss + nl
      s = s.replace(ss, rs )
# end of for ss in [ lbc, rbc, lbk, rbk ]: loop.
   s = s.replace(lbk + nl, lbk )
   s = s.replace( rbk + nl, rbk )
   s = s.replace( semi, semi + nl )
   s = s.replace( r'\n', empty_str )
   s = s.replace( rbc + nl + comma, rbc + comma + nl )
   s = search_and_replace( s, nl * 2, nl )
   s = s.strip()
comments"""
   print(s)
   if len( label_str ) > 0: print( "End: " + label_str )
   L()
   return None
# print_dict()



def plural( n, singular_str, plural_str ):
   return singular_str if n == 1 else plural_str
# plural()


def is_a_dictionary( o ):
   ts= the_type( o )
   ts = ts.lower()
   return "'dict'" in ts
# is_a_dictionary()


def is_a_list( o ):
   ts= the_type( o )
   ts = ts.lower()
   return "'list'" in ts
# is_a_list()

def is_a_tuple( o ):
   ts= the_type( o )
   ts = ts.lower()
   return "'tuple'" in ts
# is_a_tuple()
