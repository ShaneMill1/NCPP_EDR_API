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


import h5py;
import xarray;

import h2x_utils_module as U;

if U.python3():
   xrange = range
else:
   pass


def get_xarray_dataset_dict_from_hdf5_file( \
hdf5_fn, \
hdf5_dataset_list, \
 ):
# rv_d = return value dictionary
# the keys are the names of the datasets and the values
# are the dataset objects themselves.
# it is the data structure that will
# return to the caller the work accomplished by this function.
   rv_d = dict()

# get a local copy of the hdf5_dataset_list so we can
# do some evvaluation of this parameter,
# in case the caller sent in a string instead of a list, or possibly an empty list, or empty string.
   xs = hdf5_dataset_list
#
   if U.is_a_string( xs ):
      xs = xs.strip()
      xs = [] if len( xs ) == 0 else [ xs ]
   else:
      pass
#
   xs = set( xs ) # only allow unique values in this set/list.
   callers_dataset_list = list( xs )
   num_c_datasets = len( callers_dataset_list )
   caller_wants_all_datasets = num_c_datasets == 0

# try to open the .HDF5 file,
   open_mode = 'r+'

# get an h5py File object to use to reference the file.
# remember that the h5py.File object "acts" like a dictionary.
   hdf5_file = \
   h5py.File( hdf5_fn, open_mode )

# get a list of the dataset/dictionary keys
# found in the h5py file object returned by the
# h5py.File() method.
# the dictionary keys are a list of the dataset(s) names
# in the HDF5 file.
   file_dataset_list = \
   get_h5py_dataset_names_list( hdf5_file )

   num_f_datasets = len( file_dataset_list )

# make this adjustment if the caller wants to process all datasets in the hdf5 file.
   if caller_wants_all_datasets: callers_dataset_list = file_dataset_list

# now process the caller's datasets to see if they are in the hdf5 file.
   num_c_datasets = len( callers_dataset_list )

   for callers_dataset_index in xrange( num_c_datasets ):

# get the name of the caller's dataset to process.
      callers_dataset_name = callers_dataset_list[callers_dataset_index]

# some output.
      num_tabs = 1
#      print( "%sProcessing dataset (%s) (%d of %d" \
#      %( U.tab() * num_tabs, callers_dataset_name, callers_dataset_index + 1, num_c_datasets ) )

# see if the caller's dataset is a valid dataset.
      callers_dataset_is_valid = callers_dataset_name in file_dataset_list

      if callers_dataset_is_valid:
         pass
      else: # invalid dataset!
         es = "!" * 25
         ys = "Yikes! " * 10
         fn = U.get_function_name()
         fdls = str( file_dataset_list )
         U.print_this( \
         """%s
%s
In: %s()
The specified dataset (%s) is not in the set of dataset names (%s)
found in the specified hdf5 file:
%s
%s
%s
""" \
         %( \
         es, \
         ys, \
         fn, \
         callers_dataset_name, \
         fdls, \
         hdf5_fn, \
         ys, \
         es \
         ) \
         )
         continue # try the caller's next dataset.

# use the dataset name as a "key" into the h5py/hdf5 file object 
# to get a reference to the caller's dataset object.
      callers_dataset_object = hdf5_file[callers_dataset_name]

# now get help from the next function invocation
# to convert the caller's hdf5 dataset data
# into an xarray dataset.
      xarray_dataset = \
      hdf5_dataset_to_xarray_dataset( callers_dataset_object )

# save the xarray dataset for the caller.
      rv_d[callers_dataset_name] = xarray_dataset

# end of for callers_dataset_index in xrange( num_c_datasets ): loop.

# return the dictionary of xarray dataset objects to the caller.
   return rv_d
# get_xarray_dataset_dict_from_hdf5_file()


# pd = parent dictionary
# in_pd = input/initialize parent dictionary,
# the input dictionary provided by the caller that
# will be initialized by this function.
# The in_pd dictionary will buffer the hdf5 data
# that will be used by the
# xarray.Dataset.from_dict( in_pd )
# method in order to get an xarray dataset object.
# For example:
# xarray_dataset = xarray.Dataset.from_dict( in_pd )
#
def initialize_dictionary_for_input_to_xarray_Dataset_from_dict():

# initialize the "empty" parent dictionary
# that will set-up the fundamental/initial parent dictionary
# for the caller.
   empty_pd = \
   get_initial_parent_dictionary()

# send the reference to the initialized parent dictionary back to the caller.
   return empty_pd
# initialize_dictionary_for_input_to_xarray_Dataset_from_dict()


def get_h5py_dataset_names_list( hdf5_file_object ):
# The argument, hdf5_file_object is the return object reference from the
# h5py.File() method.
# rv_dataset_names_l = return value list, will return the list of dataset names to the caller.
   rv_dataset_names_l = list()
# kl = keys list.
   kl = hdf5_file_object.keys()
# process the keys list.
   for ks in kl:
      rv_dataset_names_l.append( ks )
# end of for ks in kl: loop.
#
# return the list of dataset names to the caller.
   return rv_dataset_names_l
# get_h5py_dataset_names_list()


# remember that pd or PD = parent dictionary,
# the input to the
# xarray.Dataset.from_dict()
# method.
def get_pd_keys():
# create the list.
   the_pd_keys = [ pd_coords_key(), pd_attrs_key(), pd_dims_key(), pd_data_vars_key() ]
# order them.
   the_pd_keys.sort()
# send the keys list back to the caller.
   return the_pd_keys
# get_pd_keys()


# pd_coords_key()
# provides the value of the
# "coords"
# key for accessing a pd dictionary.
def pd_coords_key():
   return the_coords_key()
# pd_coords_key()


# gets the value of the
# "attrs"
# key for accessing a pd dictionary.
def pd_attrs_key():
   return the_attrs_key()
# pd_attrs_key()


# gets the value of the
# "dims"
# key for accessing a pd dictionary.
def pd_dims_key():
   return the_dims_key()
# pd_dims_key()



# pd_data_vars_key()
# gets the value of the
# "data_vars"
# key for accessing a pd dictionary.
def pd_data_vars_key():
   return the_data_vars_key()
# pd_data_vars_key()


def cd_attrs_key():
   return the_attrs_key()
# cd_attrs_key()


def cd_dims_key():
   return the_dims_key()
# cd_dims_key()


def cd_data_key():
   return the_data_key()
# cd_data_key()


def get_initial_pd_child_dictionary():
# rv initial cd = return value initial child dictionary.
   rv_initial_cd = dict() # allocate the initial cd.
# now initialize the contents of the cd.
   rv_initial_cd[cd_dims_key()] = list()
   rv_initial_cd[cd_attrs_key()] = dict()
   rv_initial_cd[cd_data_key()] = list()
# return the initialized child dictionary to the caller.
# send the reference to the initialized child dictionary back to the caller.
   return rv_initial_cd
# get_initial_pd_child_dictionary()


def get_cd_keys():
# create the list.
   the_cd_keys = [ cd_attrs_key(), cd_data_key(), cd_dims_key() ]
# order them.
   the_cd_keys.sort()
# send the keys list back to the caller.
   return the_cd_keys
# get_cd_keys()


def get_initial_parent_dictionary():
# rv initial pd = return value initial parent dictionary.
   rv_initial_pd = dict() # allocate the initial pd.
# now initialize the contents of the pd.
   for key_str in get_pd_keys():
# the pd is composed of sub-dictionaries or child dictionaries.
      rv_initial_pd[key_str] = dict()
# end of for key_str in get_pd_keys(): loop.
# return the initialized parent dictionary to the caller.
   return rv_initial_pd
# get_initial_parent_dictionary()


def the_attrs_key():
   return "attrs"
# the_attrs_key()


def the_coords_key():
   return "coords"
# the_coords_key()


def the_dims_key():
   return "dims"
# the_dims_key()

def the_data_key():
   return "data"
# the_data_key()


def the_data_vars_key():
   return "data_vars"
# the_data_vars_key()

# new functionality
# as of 05/13/2020
#begins here.



def hdf5_dataset_to_xarray_dataset( hdf5_dataset_object ):

# OK, now here we go,
# this function is the work-horse for ingesting the hdf5 data into
# an xarray dataset object for the data-api.
# here is where we gain access to
# the data objects within the specified dataset.
   values_object_list = \
   list( hdf5_dataset_object.values() )

# now process the values object list
# in order to gain access to each data/metadata object
# that describes the data component to which it references.
   num_values = len( values_object_list )
# get this "initial" dictionary
# which will be initialized in the next values loop
# and will ultimately be the input dictionary to the:
# xarray.Dataset.from_dict()
# method/function.
# this is the whole process of converting hdf5 dataset data into an xarray dataset.
# pxd = parent xarray dictionary
   pxd_from_dict_d =  \
   initialize_dictionary_for_input_to_xarray_Dataset_from_dict()

# pebbles
# process each of the values/data/metadata objects in this dataset,
# for ingest into an xarray dataset object.
# the values come from the hdf5 file object:
# FileObject.dataset.values().
   for value_index in xrange( num_values ):
      this_value = values_object_list[value_index]
      num_tabs = 2
      value_name = str( this_value.name )

# some output.
#      print( "%sProcessing metadata variable (%d of %d) (%s): " \
#      %( U.tab() * num_tabs, value_index + 1, num_values, value_name ) )

#
# now get the component parts that we need for the specific value object (this_value).
      value_d = dict()
# this "data" may be for "precipitation", "random_error", "nv", etc, values in the dataset.
      value_d["value object"], \
      value_d["shape tuple"], \
      value_d["is a dimension"], \
      value_d["data type"], \
      value_d["name str"], \
      value_d["number of dimensions"], \
      value_d["the size"], \
      value_d["the data"] \
      = \
      get_value_components( this_value )

#      print( "%sis a dimension value: %s" \
#      %( U.tab() * (num_tabs+1), value_d["is a dimension"] ) )

# now take the values dictionary and
# properly store the value data
# in the "proper parts" of the pxd_from_dict_d dictionary
# in the dictionary that will be used as input to:
# xarray.Dataset.from_dict().
# "proper parts" = the dictionary keys: "attrs", "coords",
# "data_vars", and "dims".
      if value_d["is a dimension"]:
         make_dims_pd_dictionary_entry( value_d, pxd_from_dict_d )
         make_coords_pd_dictionary_entry( value_d, pxd_from_dict_d )
      else:
         make_attrs_pd_dictionary_entry( value_d, pxd_from_dict_d )
         make_data_vars_pd_dictionary_entry( value_d, pxd_from_dict_d )

# end of for value_index in xrange( num_values ): loop.

# now convert the hdf5 data dictionary to an xarray dataset.
   rv_xarray_dataset = xarray.Dataset.from_dict( pxd_from_dict_d )

# send the xarray dataset back to the caller.
   return rv_xarray_dataset
# hdf5_dataset_to_xarray_dataset()



def get_value_components( value_object ):
# rvrvrv return variables.
   shape_tuple = value_object.shape if U.object_has_attribute( value_object, "shape" ) else None
   is_a_dimension = len( shape_tuple ) == 1 # a logical that indicates if this value should be considered a single integer that indicates a dimension specification.
   data_type = value_object.dtype if U.object_has_attribute( value_object, "dtype" ) else None
   name_str = U.BN( str( value_object.name ) )
   number_of_dimensions = value_object.ndim if U.object_has_attribute( value_object, "ndim" ) else None
   the_size = value_object.size if U.object_has_attribute( value_object, "size" ) else None
#   the_data = value_object.values is depricated.
   the_data = value_object[()]

# send the functional results back to the caller.
   return \
   value_object, \
   shape_tuple, \
   is_a_dimension, \
   data_type, \
   name_str, \
   number_of_dimensions, \
   the_size, \
   the_data
# get_value_components()


# this function
# will store value data (vd)
# inn the parent dictionary (pxd)
# within the "dims" key dictionary
# to  indicate the dimension size for the data
# found within the vd value data dictionary.
def make_dims_pd_dictionary_entry( vd, pxd ):

# leave early if the specified value is not a dimension specification.
   if not vd["is a dimension"]: return None

# get the name of the entry to be made, the name found in the value dictionary (vd)
# this dimension name string will be like "nv", "latv", "lonv", etc.
   dimension_name_str = vd["name str"]

# get the value for the dimension.
   dimension_value = vd["shape tuple"][0] # this tuple specifies the dimension within the value dictionary.

# now prepare to make the entry,
# that is,
# save the entry dimensional information within
# the "dims" dictionary
# within the parent dictionary (pxd).

# get the key string to use to access the parent dictionary's
# child dictionary (the dims child dictionary).
# pd = parent dictionary, cd = child dictionary, ks = key string.
   pd_cd_ks = pd_dims_key()

# finally update/store/"make the entry"
   pxd[pd_cd_ks][dimension_name_str] = dimension_value

# now return to the caller.
   return None
# make_dims_pd_dictionary_entry()


# this function
# will initialize a dictionary
# with the value data (vd)
# and then store that data in a dictionary
# which will be placed inn the parent dictionary (pxd)
# within the "coords" key dictionary.
def make_coords_pd_dictionary_entry( vd, pxd ):

# leave early if the specified value is not a dimension specification.
   if not vd["is a dimension"]: return None

# get the name of the entry to be made, the name found in the value dictionary (vd)
# this "ks" will be like "nv", "latv", "latv", "time", etc.
# basically "metadata" entries.
   entry_ks = vd_ks = vd["name str"]

# get a dictionary to hold "this" entry's information.
# this child dictionary,
# the entry dictionary,
# will eventually be placed in
# pxd["coords"][entry_ks]
   entry_d = get_initial_pd_child_dictionary()

# now prepare to make the entry,
# that is,
# save the entry dictionary within
# the "coords" dictionary
# within the parent dictionary (pxd).

# now start filling in the entry dictionary.

# first get the attrs dictionary.
   entry_d[cd_attrs_key()] = \
   get_coords_child_attrs_dictionary( vd )

# now get the data list.
   entry_d[cd_data_key()] = \
   get_coords_child_data_list( vd )


# now get the dims list.
   entry_d[cd_dims_key()] = \
   get_coords_child_dims_list( vd )

# finally update/store/"make the entry"
   pxd[pd_coords_key()][entry_ks] = entry_d

# now return to the caller.
   return None
# make_coords_pd_dictionary_entry()


def get_coords_child_attrs_dictionary( vd ):
# rv_d = return value dictionary.
# will contain the attributes found in the vd value dictionary value object.
   rv_d = dict()

# get the value object (vo) to process for attributes.
   vo = vd["value object"]

# al = attributes list.
# a list of "strings" that name the individual attributes.
   al = list( vo.attrs )

# now process the attributes list (al)
# to initialize the attributes dictionary.
   for key_str in al:
      attribute_value = str(vo.attrs[key_str]) # provides access to the attribute.
      attribute_value=attribute_value.replace("b'",'') #added conversion from byte to string because zarr couldn't handle byte
      attribute_value=attribute_value.replace("'",'')
      rv_d[key_str] = attribute_value # store the attribute in the dictionary.
# end of for key_str in al: loop.

# return the initialized dictionary back to the caller.
   return rv_d
# get_coords_child_attrs_dictionary()


def get_coords_child_data_list( vd ):
# rv_l = return value list.
# will contain the data found in the vd value dictionary value object.
   rv_l = list()

# now process the data list to gather the data for the caller.
   for i in xrange( vd["the size"] ):
      rv_l.append( vd["the data"][i] ) # store the data element.
# end of for i in xrange( vd["the size"] ): loop.

# return the initialized lisst back to the caller.
   return rv_l
# get_coords_child_data_list()


def get_coords_child_dims_list( vd ):

# rv_l = return value list.
# will contain the data found in the vd value dictionary value object
# rellative to the names of the dimensions for the particular
# data object,
# probably a numpy object.
# get this "helper" function to do the work for us.
   rv_l = \
   get_value_attr_info_list( value_object = vd["value object"], \
   attr_name = "DimensionNames" )

# return the initialized lisst back to the caller.
   return rv_l
# get_coords_child_dims_list()


def get_value_attr_info_list( value_object, attr_name ):

# rv_l = return value list,
# will return the information list about
# the specified attrribute to the caller.
   rv_l = list()

# access the value's attributes.
   attrs_object = value_object.attrs

# this provides the names of the attributes.
# print this list if you need to see them.
   attrs_names_list = list( attrs_object )

# access the value of the particular attribute.
   attr_info = attrs_object[attr_name]
   attr_info_str = str( attr_info )
   xl = attr_info_str.split( "'", 1 )
   xs = xl [-1]
   xs = xs.replace( "'", U.empty_str() )
   xs = xs.strip()

   attr_info_str = xs
# split the string into a list for individual processing/saving for the caller.
   xl = attr_info_str.split( "," )

# process the list.
   for xs in xl:
      rv_l.append( xs )
# end of for xs in xl: loop.

#   print("@@@@@@@@@")
#   print(attr_info)
#   print(attr_info_str)
#   print(attrs_names_list)
#   print( value_object.name )
#   print( rv_l )
#   exit(0)
# return the list to the caller.
   return rv_l
# get_value_attr_info_list()


# this function
# does nothing at this time.
def make_attrs_pd_dictionary_entry( vd, pxd ):

# leave early if the specified value is a dimension specification.
   if vd["is a dimension"]: return None

# nothing done here at this time.

# now return to the caller.
   return None
# make_attrs_pd_dictionary_entry()


# this function
# will initialize a dictionary
# with the value data (vd)
# and then store that data in a dictionary
# which will be placed inn the parent dictionary (pxd)
# within the "data_vars" key dictionary.
def make_data_vars_pd_dictionary_entry( vd, pxd ):

# leave early if the specified value is a dimension specification.
   if vd["is a dimension"]: return None

# get the name of the entry to be made, the name found in the value dictionary (vd)
# this "ks" will be like "nv", "precipitation", "random_error", etc.
   entry_ks = vd["name str"]

# get a dictionary to hold "this" entry's information.
   entry_d = get_initial_pd_child_dictionary()

# store the entry's attrs information.
   entry_d[cd_attrs_key()] = get_coords_child_attrs_dictionary( vd )

# store the data, the hdf5 data object.
# might be a numpy data object.
   entry_d[cd_data_key()] = vd["the data"]

# store the entry's dims information.
   entry_d[cd_dims_key()] = get_coords_child_dims_list( vd )

# now save the entry dictionary within
# the "data_vars" dictionary
# within the parent dictionary (pxd).
   pxd[pd_data_vars_key()][entry_ks] = entry_d

# return to the caller.
   return None
# make_data_vars_pd_dictionary_entry()
