"""
File:
hdf5_to_xarray_module.py

Created:
02/14/2020
author:
joseph.lang@noaa.gov

Purpose:
The purpose of this file/module is to provide the functionality to process an hdf5 file
in order to get the hdf5 grou data converted into
xarray dataset data.
The entry point into this module is via the

get_xarray_dataset_dictionary_from_hdf5_file(
the_hdf5_input_filename,
)
function.

The arguments
the_hdf5_filename
specifies the name of the hdf5 file to be processed
and

The return value is a dictionary of xarray datasets
where the keys are the name of the hdf5 group that provided
the data that was converted into the specific xarray dataset.

The hdf5 to xarray dataset conversion is performed by
xarray.Dataset.from_dict()
method.

Here is the "parent dictionary"
the input dictionary to the
xarray.Dataset.fro_dict()
method:
parent_dictionary
{

'attrs':
 {
}, # end of attrs dictionary

 'coords':
 {
},  # end of coords dictionary

'data_vars':
 { # this data_vars dictionary should contain one or more "child dictionaries" that describe the dataset(s) in the hdf5 group being processed/ingested.

'first_dataset_dictionary_for_data_vars': # this would be for 'precipitation" for nasa accum precip hdf5 data, or "lat" for wildfire hdf5 data, etc.
{

'attrs':
{
}, # end of attrs dictionary

 'data':
[
], end of data list.

 'dims':
[
] # end of dims list.

} # end of first dataset dictionary for data_vars

'next_dataset_dictionary_for_data_vars': # for the next dataset being ingested.
{

'attrs':
{
}, # end of attrs dictionary

 'data':
[
], # end of data list.

 'dims':
[
] # end of dims list.

} # end of next dataset dictionary for data_vars

more data_vars child dictionaries for subsequent datasets would follow.

},  # end of data_vars dictionary

'dims':
 {
} # end of dims dictionary

} # end of parent_dictionary

"""

import h5py;
import xarray;

import h2x_utils_module as U;
# the following list will buffer the ingest config dictionary's "keys"
# in the order that they are loaded from the configuration file.
# this list will provide "ordering" of the ingest configuration data
# dictionary as it appears in the ingest config file.
g_ingest_config_kl = list()

# Remember:
# pd = pxd = parent xarray dictionary, the input to the xarray.Dataset.from_dict() method.
#
# The pd is the data structure that organizes all of the dataset(s) within an hdf5 group
# to be processed by the xarray.Dataset.from_dict() method
# to create an xarray dataset.

# vd = value dictionary.
# h2x = hdf5 to x array
# this class supports the transition of processing the hdf5 hierarchy to gain
# access to the datasetswithin the hdf5 file being processed.
class h2x_support_object:

# here is the constructor method.
   def __init__( self, in_parent_object, in_parent_name, in_value_object, in_hdf5_filename ):
      self.parent = in_parent_object
      self.parent_name = in_parent_name
      self.value = \
      self.value_object = in_value_object
      self.my_name = self.value.name
      self.hdf5_filename = in_hdf5_filename
      return None
# __init__()

# the destructor method.
   def __del__( self ):
      return None
# __del__()


# end of class h2x_support_object:

# special support for old python 2.7 code in python 3.
if U.python2():
   pass
else:
   xrange = range


# :f1:
# this function is the entry point for this module and
# where external logic will make reference to this module in order to produce
# an xarray dataset by processing the specified hdf5 file.
#
# The name tells it all.
def get_xarray_dataset_from_hdf5_file(the_hdf5_input_filename,ingest_config_fn):

   def process_hdf5_object( \
   the_root_hdf5_object, \
   the_hdf5_object, \
   groups_datasets_dict, \
   group_dataset_dict, \
   ):
#
      is_a_file = U.is_hdf5_file_object( the_hdf5_object )
#
      is_a_group = U.is_a_group_object( the_hdf5_object )
#
      is_a_dataset = U.is_a_dataset_object( the_hdf5_object )
#
      if is_a_file or is_a_group:
#
         for object_key in the_hdf5_object.keys():
            process_hdf5_object( the_root_hdf5_object, the_hdf5_object[object_key], \
            groups_datasets_dict, \
            group_dataset_dict, \
         # end of for object_key in the_hdf5_object.keys(): loop.
            )
#
      elif is_a_dataset:
# Yahoo! We have a dataset (the_hdf5_object)!
# use some object references to ease syntax.
# hds = hdf5 dataset
         hds = the_hdf5_object
         the_parent = hds.parent
         parent_name = the_parent.name
         hdf5_filename = the_root_hdf5_object.filename

# concern for the "root dataset" arose from our processing the wildfire hdf5 file.
# the "root" of that hdf5 file housed
# 2 datasets (lat and lon) and 2 groups (ancill and emissions),
# therefore the name of the dataset's parent was "/"
# some adjustments had to be made so that our
# dataset naming scheme continues to work,
# that is so that the dataset name should be
# "/lat" or "/lon" not "/".
         is_root_dataset = parent_name == "/"
         if is_root_dataset: parent_name = hds.name

# hdsd = hdf5 dataset dictionary
# this dictionary will have keys that refer to a group in the hdf5 file being processed
# and the associated values will be the list of dataset(s)
# in the particular group referred to by the key.
         hdsd = groups_datasets_dict

# this dictionary is provided to allow
# direct access to a dataset's data given the full path to any
# dataset in the hdf5 file being processed.

# provide this reference to ease syntax
# g2d = group to dataset.
         g2d = group_dataset_dict

#
         hdsd_id = parent_name
# some logicals to indicate the "old " ness" or new ness of "this" dataset.
         is_old_dataset = hdsd_id in hdsd
         is_new_dataset = not is_old_dataset

# store the current dataset info in 2 different places for future uses.
# the first place store the data in group referenced lists of datasets.
# these groups are used to generate the hierarchy report when the report mode is selected for the process.
         if is_new_dataset:
            values_list = hdsd[hdsd_id] = list()
         else:
            values_list = hdsd[hdsd_id]

# get an object in which to store the value information about this hdf5 dataset.
         value_object = \
         h2x_support_object( the_parent, parent_name, hds, hdf5_filename )

# save the value object for later use; this object references hdf5 dataset(s) like nv, latv, lonv, precipitation, random_error, for the nasa accumulated precipitation data/metadata.
         hdsd[hdsd_id].append( value_object )

# second place used to generate an xarray dataset from hdf5 data.
# that is, the full group name for the dataset is the key to referencing
# the value that refers to the dataset information.
# store in a list to be consistent with the hdsd dictionary.
         g2d[value_object.my_name] = [  value_object ]
# end of the elif is here.

      return None
# process_hdf5_object()

# alpha
# start begin get_xarray_dataset_dictionary_from_hdf5_file()
   U.E()

#the_hdf5_report_group
#specifies where in the hdf5 hierarchy 
#the hdf5 hierarchy report should begin when the "-report" command line
# argument is specified.
# we always want this value to be '/' so that the generated report
# will produce reporting data on the entire hdf5 file hierarchy.
   the_hdf5_report_group = '/'

#jl   print("fffffffffffffff")
#jl   print( "hdf5 input filename:\n(%s)" %( the_hdf5_input_filename ) )
#jl   print( "hdf5 report group:\n(%s)" %( the_hdf5_report_group ) )
#jl   print("gggggggggggggggggggg")

# open the hdf5 input file for processing.
   the_hdf5_f = U.open_hdf5_file( the_hdf5_input_filename )

# here is where will store the group(s) and related dataset(s) information for the specified input hdf5 file.
# this dictionary will gather all datasets associated with a particular group
# in a dictionary key/value. arrangement
# where the key is the "full" group name and the associated value
# is a list of datasets within the associated group.
# please note that all entries within this dictionary are absolute references to the objects within the hdf5 hierarchy and
# memory usage is minimal.
   groups_datasets_dictionary = dict()

# the next dictionary is provided to provide a one-to-one association
# between a group specification for a dataset and a direct connection
# via a reference to the dataset that the group specifies.
# once again, this structure merely contains references and memory usage should be of little concern.
# This dictionary is provided to simplify code access to hdf5 reference data objects.
   group_dataset_dictionary = dict()

# let the processing begin.
   process_hdf5_object( \
   the_hdf5_f, \
   the_hdf5_f[the_hdf5_report_group], \
   groups_datasets_dictionary, \
   group_dataset_dictionary, \
   )

# now see if the reason for executing this script is to view
# the report that describes the hdf5 hierarchy of the input hdf5 file.
# if the report is generated,
# this script will be terminated so that the report can ve reviewed.
   if generate_hdf5_hierarchy_report( the_hdf5_input_filename, groups_datasets_dictionary  ):
      return None # the report was generated, so leave now.
   else:
      pass


# shane new function as of 08/12/2020
# here we will load the "ingest configuration dictionary" with the data from the file
# that was specified on the command line.
# the keys for the dictionary are the complete names of the datasets
# found in the hdf5 group and
# each value is a list of commands to be executed by the python interpreter
# to store the information for each dataset
# in the parent dictionary.
   ingest_config_dictionary = \
   load_ingest_configuration_dictionary_from_file(ingest_config_fn)

# now process the hdf5 groups/datasets that we have accumulated
# (by processing the specified hdf5 file)
# based on the groups/datasets specified in the ingest configuration file
# to derive the desired xarray dataset.
   rv_xarray_dataset = \
   process_hdf5_groups_dictionary_to_derive_xarray_dataset( \
   ingest_config_dictionary, \
   the_hdf5_f, \
   group_dataset_dictionary, \
   )

# we are done with this file so close it.
   the_hdf5_f.close()

#jl   print("yyyyyyyyyyy")

#
   U.L()

#
   return rv_xarray_dataset
# get_xarray_dataset_from_hdf5_file()


# :f2:
def process_hdf5_groups_dictionary_to_derive_xarray_dataset( \
ingest_config_d, \
hdf5_file_object, \
group_dataset_dict, \
):

   global g_ingest_config_kl; # will provide proper ordering of the ingest config dictionary.

#
   U.E()

# some output preparation.
   tabs = 2

# modifications dated: 10/07/2020 20201007.

# get this "initial" dictionary
# the "parent dictionary"
# which will be initialized by this function
# and will ultimately be the input dictionary to the:
# xarray.Dataset.from_dict()
# method/function.
# this is the entire process of converting an hdf5 group's dataset components data (dataset(s))
# into an xarray dataset.
# albert
   GroupDictionary = \
   ParentDictionary = \
   pd_get_initial_parent_dictionary()

# see if we should output the initial parent dictionary.
   U.print_dict( ParentDictionary, "initial parent dictionary" )

# now fill  in the parent's attrs dictionary
# with attribute information for the hdf5 file.
# The attributes provided in the root ("/") of the hdf5 file hierarchy.
   pd_init_attrs_dictionary( ParentDictionary, hdf5_file_object["/"] )

# now access and process the desired hdf5 groups and datasets
# that were specified in the ingest configuration file/dictionary.
   groups_to_ingest = len( g_ingest_config_kl )

   group_count = 0
   for ingest_group in g_ingest_config_kl:
# set a reference to a previously named object for my convenience.
# increment the goup counter.
      group_count += 1

      desired_hdf5_group = ingest_group

      print( "%sIngest Config Group: (%s) %d of %d" %( I( tabs ), ingest_group, group_count, groups_to_ingest ) )
# make sure that the specified group is in the hdf5 input file data.
      if ingest_group in group_dataset_dict:
         pass # all is well, so move on to ingesting the group data.
      else: # display an error about requesting a group that does not exist and terminate this process.
         print( "Error! " * 5 )
         print( "No HDF5 group was found for  (%s) in the HDF5 input file!" %( ingest_group ) )
         print( "Make appropriate adjustments in the ingest config file for this group and\nrerun this program!" )
         exit( 1 )

# get a reference to the dataset list for the desired group within the hdf5 file.
# dsl = dataset list;
# which is a list of h2x_support_object objects.
      dsl = group_dataset_dict[desired_hdf5_group]
      num_datasets = len( dsl )
      dataset_counter = 0

# now process the dataset(s) for the desired hdf5 group.
# ds = dataset.
      for ds in dsl:
# count this dataset.
         dataset_counter += 1
         ds_name = ds.my_name
         print( "%sProcessing group/dataset %d of %d (%s)" \
         %( I( tabs + 2 ), dataset_counter, num_datasets, ds_name ) )

# now ingest this dataset's data.
         dataset_to_group_dictionary( \
         ingest_config_d[ds_name], \
         ParentDictionary, \
         ds, \
         tabs + 4, \
         )

# end of for ds in dsl: loop.

# end of for ingest_group in g_ingest_config_kl: loop.

# see if we should output the completed parent dictionary.
   U.print_dict( ParentDictionary, "completed parent dictionary" )

# should we convert the hdf5 data dictionary to an xarray dataset?
   joe( GroupDictionary )
   if U.skip_from_dict():
      derived_xarray_dataset = None
   else:
# derive the x array dataset for the caller.
      derived_xarray_dataset = xarray.Dataset.from_dict( ParentDictionary )

#
   U.L()

#
   return derived_xarray_dataset
# process_hdf5_groups_dictionary_to_derive_xarray_dataset()


# :f3:
def dataset_to_group_dictionary( \
ingest_code_l, \
ParentDictionary, \
this_dataset, \
tabs \
):

# local functions.
   def get_attributes():
      attributes_d = get_coords_child_attrs_dictionary( this_dataset.value_object )
      return attributes_d
   # get_attributes()


   def get_data():
#jl      print("dddddddddddd")
#jl      print("TheType (this_dataset): %s" %( U.the_type( this_dataset ) ) )
#jl      print("my name (this dataset): (%s)" %( this_dataset.my_name ))
#jl      print("%s" %( U.get_object_dir_str( this_dataset ) ) )
#jl      print("eeeeeeeeeeee")
#xx      value_d = get_value_components( this_dataset.value )
#xx      print( "the type (value_d components of value): %s" %( U.the_type( value_d )))
#xx      print("%s" %( U.get_object_dir_str( value_d ) ) )
#jl      print("ffffffffffff")
#xx      data_list = get_data_list( value_d )
#
# getting to the data.
      v = this_dataset.value
#jl      print("v type: %s" %( U.the_type(v)))
#jl      print("dir list\n%s" %( U.get_object_dir_str(v)))
#jl      print("hhhhhhhhhhhhhhhhh")
# wwwwww H5pyDeprecationWarning:
# wwwwwwdataset.value has been deprecated. Use dataset[()] instead.
# wwwwww      vv = v.value[()]
      vv = v[()]
#jl      print("vovovovovovovovovovovovovovovo")
#jl      print("type vv %s" %( U.the_type(vv)))
#jl      print("dir list:\n%s" %( U.get_object_dir_str(vv)))
#jl      print("wowowowowowowowowowowowowowowo")
      return vv
      print("dddddddddddd")
      print("%s" %( U.get_object_dir_str( this_dataset ) ) )
      print("eeeeeeeeeeee")
      print("%s" %( U.get_object_dir_str( this_dataset.value ) ) )
      print("ffffffffffff")
      exit(0)
      return data_list
#kk#      ld = this_dataset
#kk#      ln = 35
#kk#      print("t" * ln )
#kk#      print( "%s" %( U.get_object_dir_str( ld ) ) )
#kk#      print("u"*ln)
#kk#      return ld.value
#      return this_dataset[()]
# qwerty.
      vo = this_dataset.value_object
      print("ttttttttt")
      print("%s" %( U.the_type( vo ) ) )
      print("ssssssssssss")
      print("%s" %( U.get_object_dir_str( vo ) ) )
      print("uuuuuuuuuuu")
      v = vo.value
      print("vvvvvvvvvvv")
      print("%s" %( U.the_type( v ) ) )
      print("xxxxxxxxxxx")
      print("%s" %( U.get_object_dir_str( v ) ) )
      print("zzzzzzzzzzz")
      return v
# reason for the return statement as it appears:
#DeprecationWarning: dataset.value has been deprecated. Use dataset[()] instead.
      return vo[()]
# get_data()

# start begin dataset_to_group_dictionary()
#
   U.E()
#
# record these values for config file references.
   this_dataset_name = U.BN( this_dataset.my_name )
   dsn = \
   dataset_name = U.BN( this_dataset_name )
#
# Let this reference happen for clarity.
   GroupDictionary = ParentDictionary
   for ingest_code_s in ingest_code_l:
# quick edit.
      ingest_code_s = ingest_code_s.strip()
      if len( ingest_code_s ) == 0: continue # skip blank lines.
#
#jl      print( "%sPython Executing: (%s)" 
#jl      %( I( tabs ), ingest_code_s ) )
#
# now compile and execute the python (ingest_code_s) code.
      compile_file = "/tmp/jl.error.txt"
      compiled_object = compile( ingest_code_s, compile_file, 'exec' )
      exec( compiled_object )
#
# end of for ingest_code_s in ingest_code_l: loop.
#
   U.L()
#
   return None
# dataset_to_group_dictionary()
def convert_hdf5_group_to_xarray_dataset( \
hdf5_data_object_list, \
):

   U.E()
# OK, now here we go,
# this function is the work-horse for ingesting the hdf5 data into
# an xarray dataset object for the data-api.

# here is where we gain access to
# the data objects within the specified dataset(s) grouping
# where the data values are actually stored.

# use these references to simplify syntax.
# tdl = the data list
   values_list = \
   tdl = hdf5_data_object_list

   num_values = len( values_list )
   object0 = values_list[0] # get a reference to the initial value in the values_list.

   group_object = \
   the_parent = object0.parent # for access to the parent's attrs attribute later.

# now process the values object list
# in order to gain access to each data/metadata object
# that is each hdf5 dataset within the hdf5 group,
# that describes the data component to which it references.

# get this "initial" dictionary
# which will be initialized by this function
# and will ultimately be the input dictionary to the:
# xarray.Dataset.from_dict()
# method/function.
# this is the entire process of converting an hdf5 group's dataset components data (dataset(s))
# into an xarray dataset.
# pxd = parent xarray dictionary
# the dictionary that xarray will process to produce an xarray dataset.
# the processing will be done by, once again:
# xarray.Dataset.from_dict()
# to create an xarray dataset from the hdf5 file's group and associated dataset object(s).
# pxd = parent xarray dictionary
# elbert
   ParentDictionary = \
   pxd =  pd_get_initial_parent_dictionary()

   U.print_dict( pxd, "initial parent dictionary" )

# now fill  in the parent's attrs dictionary.
   pd_init_attrs_dictionary( pxd, group_object )

# barney
# process each of the values/data/metadata dataset objects in this hdf5 group,
# for ingest into an xarray dataset object.
# the values come from the hdf5 file object:
# FileObject["the hdf5 group name"].dataset_list objects().
   for value_index in xrange( num_values ):
# the value is the dataset value/data for the current group being processed.
      this_value = values_list[value_index]
      value_name = str( this_value.my_name )

# some output.
      tabs =3
      ts = U.tab() * tabs
      print( "%sProcessing data variable (%d of %d) (%s): " \
      %( ts, value_index + 1, num_values, value_name ) )

# now get the component parts that we need/want for the specific value object (this_value).
# this "data" may be for "precipitation", "random_error", "nv", etc,
# values in the dataset group for the NASA accumulated precipitation hdf5 file.
# Of course, this data depends on the hdf5 file's group being processed.
      value_d = get_value_components( this_value.value )

# now take the values dictionary and
# properly store the value data
# in the data_vars sub-dictionary of the pxd dictionary
# the dictionary that will be used as input to:
# xarray.Dataset.from_dict() method.
# within the pxd dictionary.
# note, the "data_vars" function will direct logic to make entries within the
# "dims" and "coords" sub-dictionaries of the pxd parent dictionary.
      make_pd_data_vars_dictionary_entry( pxd, value_d )
#zach
#      make_pd_dims_dictionary_entry( value_d, pxd )
#      make_pd_coords_dictionary_entry( value_d, pxd )

# end of for value_index in xrange( num_values ): loop.

# should we send the "from dict" dictionary to the standard output for review?
   U.print_dict( pxd, "completed parent dictionary" )

   U.JSP( pxd, "completed parent dictionary" )
# should we convert the hdf5 data dictionary to an xarray dataset?
   mr_joe( pxd )
   if U.skip_from_dict():
      rv_xarray_dataset = None
   else:
# now convert the hdf5 data dictionary to an xarray dataset.
      rv_xarray_dataset = xarray.Dataset.from_dict( pxd )

# send the xarray dataset back to the caller.
   return rv_xarray_dataset
# convert_hdf5_group_to_xarray_dataset()


def pd_init_attrs_dictionary( \
pd, \
the_group, \
):

# now initialize the "attrs" dictionary (the "attrs" child dictionary (sub-dictionary)) within the parent dictionary (pd).
# but see if attributes (attrs) are provided for this group.
   make_pd_attrs_dictionary_entry( pd, the_group )

# save the group name in the specified attrs dictionary.
   U.store_value_in_dictionary( \
   the_dict = pd[pd_attrs_key()], \
   the_key = ".HDF5 group:",
the_value = the_group.name, \
   )

# save the hdf5 file name in the specified attrs dictionary.
   U.store_value_in_dictionary( \
   the_dict = pd[pd_attrs_key()], \
   the_key = ".HDF5 Filename:",
the_value = the_group.file.filename, \
   )

# save the current date/time (the ingest date/time) in the specified attrs dictionary.
   U.store_value_in_dictionary( \
   the_dict = pd[pd_attrs_key()], \
   the_key = ".HDF5 Ingest Date/Time:", \
the_value = U.time.strftime( "%Y%m%d%H%M%S", U.time.gmtime() ), \
   )

# return the populated parent dictionary reference (pd) to the caller.
   return pd
# pd_init_attrs_dictionary()

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


def pd_get_initial_parent_dictionary():
# rv initial pd = return value initial parent dictionary.
   rv_initial_pd = dict() # allocate the initial pd.
# now initialize the contents of the pd.
   for key_str in get_pd_keys():
# the pd is composed of sub-dictionaries or child dictionaries.
      rv_initial_pd[key_str] = dict()
# end of for key_str in get_pd_keys(): loop.
# return the initialized parent dictionary to the caller.
   return rv_initial_pd
# pd_get_initial_parent_dictionary()


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
def get_value_components( value_object ):
# rvvd = return value value dictionary
   rvvd = dict()

# fill in the component parts of the dictionary to be returned to the caller.
   rvvd["value object"] = value_object
   rvvd["shape tuple"] = shape_tuple = value_object.shape if U.object_has_attribute( value_object, "shape" ) else None
   rvvd["data type"] = data_type = value_object.dtype if U.object_has_attribute( value_object, "dtype" ) else None
   rvvd["name str"] = name_str = U.BN( str( value_object.name ) )

   rvvd["number of dimensions"] = number_of_dimensions = value_object.ndim if U.object_has_attribute( value_object, "ndim" ) else 0


   rvvd["single"] = \
   rvvd["single dimensional"] = True if number_of_dimensions < 2 else False
   rvvd["multi"] = \
   rvvd["multi dimensional"] = not rvvd["single dimensional"]
   rvvd["the size"] = the_size = value_object.size if U.object_has_attribute( value_object, "size" ) else None

#   the_data = value_object.values is depricated.
# joseh.lang@noaa.gov made these changes on 08/21/2020.
#jl#   rvvd["the data"] =    the_data = value_object[()]
   the_data_object = value_object
   rvvd["the data object"] = the_data_object
   the_data = the_data_object[()]
   rvvd["the data"] = the_data

# send the functional results back to the caller.
   U.print_dict( rvvd, name_str + " component part value object dictionary" )

   return rvvd
# get_value_components()


# this function
# will store value data (vd)
# inn the parent dictionary (pd)
# within the "dims" key dictionary
# to  indicate the dimension size for the data
# found within the vd value data dictionary.
def make_pd_dims_dictionary_entry( pd, vd ):

# get the name of the entry to be made, the name found in the value dictionary (vd)
# this dimension name string will be like "nv", "latv", "lonv", etc.
   dimension_name_str = vd["name str"]

# get the value for the dimension.
   dimension_value = vd["shape tuple"][0] # this tuple specifies the dimension within the value dictionary.

# now prepare to make the entry,
# that is,
# save the entry dimensional information within
# the "dims" dictionary
# within the parent dictionary (pd).

# get the key string to use to access the parent dictionary's
# child dictionary (the dims child dictionary).
# pd = parent dictionary, cd = child dictionary, ks = key string.
   pd_cd_ks = pd_dims_key()

# finally update/store/"make the entry"
   pd[pd_cd_ks][dimension_name_str] = dimension_value

# now return to the caller.
   return None
# make_pd_dims_dictionary_entry()


# this function
# will initialize a dictionary
# with the value data (vd)
# and then store that data in a dictionary
# which will be placed inn the parent dictionary (pd)
# within the "coords" key sub-dictionary.
def make_pd_coords_dictionary_entry( pd, vd ):

# get the name of the entry to be made, the name found in the value dictionary (vd)
# this "ks" will be like "nv", "latv", "latv", "time", etc.
# basically "metadata" entries.
   entry_ks = vd_ks = vd["name str"]

# get a dictionary to hold "this" entry's information.
# this child dictionary,
# the entry dictionary,
# will eventually be placed in
# pd["coords"][entry_ks]
   entry_d = get_initial_pd_child_dictionary()

# now prepare to make the entry,
# that is,
# save the entry dictionary within
# the "coords" dictionary
# within the parent dictionary (pd).

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

# finally update/store/"make the entry in the parent dictionary"
   pd[pd_coords_key()][entry_ks] = entry_d

# now return to the caller.
   return None
# make_pd_coords_dictionary_entry()


def get_coords_child_attrs_dictionary( vd ):
# rv_d = return value dictionary.
# will contain the attributes found in the vd value dictionary value object.
   rv_d = dict()

# get the value object (vo) to process for attributes.
   vo = vd["value object"] if U.is_a_dictionary( vd ) else vd

# al = attributes list.
# a list of "strings" that name the individual attributes.
   al = list( vo.attrs )

# now process the attributes list (al)
# to initialize the attributes dictionary.
   for key_str in al:
      attribute_value = str(vo.attrs[key_str])
      attribute_value = U.bytes_object_to_str_object( attribute_value ) # added on 05/15/2020 by joseph.lang@noaa.gov given Shane's (shane.mill@noaa.gov) findings and xarr needs for a string object not bytes.
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
   num_data_objects = vd["the size"]

   print("o"*49)
   print(vd)
   print( "num data objects: %d\ndata type: %s" %( num_data_objects, U.the_type( vd["the data"] ) ) )
   U.F()
   print("s"*49)
   for i in xrange( num_data_objects ):
      rv_l.append( vd["the data"][i] ) # store the data element.
# end of for i in xrange( num_data_elements ): loop.

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
#   print("kkkkkkkkkkkkkkkkkkkkk")
#   print(attrs_names_list)
#   print("ttttttttttttttttttttt")

# watch for not having the specified attribute name string.
   if attr_name in attrs_names_list:
      pass # all is good.
   else: # not so good.
      return rv_l # send the empty list back to the caller.


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

# return the list to the caller.
   return rv_l
# get_value_attr_info_list()


# this function
# will provide attribute (attrs) information
# for the specified dataset
# if the dataset actually has attributes.
# what I mean is there a
# hdf5_dataset_object.attrs
# component/object/ method/attribute in the supplied dataset object.
def attrs_to_dict( attrs_object, the_dict = None ):

# do we need to allocate a dictionary?
   if the_dict == None: the_dict = dict()
# the object reference (a) will simplify syntax for me.
   a = attrs_object

# get the list of keys for the attrs entries.
   kl = list( a )

   for ks in kl:
      the_dict[ks] = a.get( ks )
# end of for ks in kl: loop.

   return the_dict
# attrs_to_dict()


def make_pd_attrs_dictionary_entry( pd, hdf5_object ):

# get a reference to the attrs dictionary of concern.
   pd_attrs_d = pd[pd_attrs_key()]

# nothing to do if there is no attrs information...
   if U.object_has_attribute( hdf5_object, "attrs" ):
      pass # this is good, so continue in this function.
   else: # not as good, make a note, then leave this function.
      group_name = hdf5_object.name
      pd_attrs_d["SpecialNote"] = "Group (%s) has no attrs." %( group_name )
      return None

# get the attributes for this hdf5 group.
   attrs_to_dict( hdf5_object.attrs, pd_attrs_d )

# return to the caller.
   return None
# make_pd_attrs_dictionary_entry()


# this function
# will initialize a dictionary
# with the value data (vd)
# and then store that data in a dictionary
# which will be placed inn the parent dictionary (pd)
# within the "data_vars" key dictionary.
def make_pd_data_vars_dictionary_entry( pd, vd ):

# get the name of the entry to be made, the name found in the value dictionary (vd)
# this "ks" will be like "nv", "precipitation", "random_error", etc;
# the name of the dataset.
   entry_ks = vd["name str"]

# get a dictionary to hold "this" entry's information.
   entry_d = get_initial_pd_child_dictionary()

#jl   print("JJJJJJ")
#jl   print('entry ks')
#jl   print("(%s)" %( entry_ks))
   U.print_dict( entry_d, "initial child dictionary" )
#jl   print("KKKKKKKKK")
#jl   exit(0)

# store the entry's attrs information.
   entry_d[cd_attrs_key()] = get_coords_child_attrs_dictionary( vd )

# store the data, the hdf5 data object.
# might be a numpy data object.
   entry_d[cd_data_key()] = vd["the data"]
# store the entry's dims information.
   entry_d[cd_dims_key()] = get_coords_child_dims_list( vd )

# now save the entry dictionary within
# the "data_vars" dictionary
# within the parent dictionary (pd).
   pd[pd_data_vars_key()][entry_ks] = entry_d

# return to the caller.
   return None
# make_pd_data_vars_dictionary_entry()


# added by joseph.lang@noaa.gov on 08/04/2020.
def I( num_indents = 1 ):
   return indenter( num_indents )
# I()


def indenter( n = 1 ):
   return U.space() * n
# indenter()

def generate_hdf5_hierarchy_report( \
hdf5_fn, \
hdf5_groups_dictionary, \
):

# local functions for reporting logic.
   def jl_bytes( o ):
      ts = U.the_type( o )
      ts = ts.lower()
      is_bytes = 'bytes' in ts
      if not is_bytes: return o
      s = str( o )
      if s.startswith( 'b' ): s = s[1:]
      if s.startswith( "'" ): s = s[1:]
      if s.endswith( "'" ): s = s[:-1]
      return s
# jl_bytes()

   def jl_edit( s, indent ):
      ss = '\\n'
      nl = U.new_line()
      indent_str = indenter( indent )
      if s.count( ss ) == 0: return indent_str + s
      s = indent_str + s
      s = s.replace( ss, nl + indent_str )
      return s
# jl_edit()


# vo = value object for the dataset being processed.
   def jl_dim1_only1( v ):
      rvs = str( v.tolist()[0] )
      return rvs
   # jl_dim1_only1()


   def jl_dim1( v ):
      t = v.shape
      n0 = t[0]
      dl = v.tolist()
      rvs = U.empty_str()
      sp = U.space()
#
      max_n = 5
      n = 0
      break_out = False
      for i in range( n0 ):
         n += 1
         break_out = n > max_n
         if break_out: break
         xs = str( dl[i] )
         if n > 1: rvs += sp
         rvs += xs
# end of for i in range( n0 ): loop.
#
      return rvs
   # jl_dim1()


   def jl_dim2( v ):
      t = v.shape
      n0 = t[0]
      n1 = t[1]
      dl = v.tolist()
      rvs = U.empty_str()
      sp = U.space()
#
      max_n = 5
      n = 0
      break_out = False
      for i in range( n0 ):
         if break_out: break
         for j in range( n1 ):
            n += 1
            break_out = n > max_n
            if break_out: break
            xs = str( dl[i][j] )
            if n > 1: rvs += sp
            rvs += xs
# end of for j in range( n1 ): loop.
# end of for i in range( n0 ): loop.
#
      return rvs
   # jl_dim2()


   def jl_dim3( v ):
      t = v.shape
      n0 = t[0]
      n1 = t[1]
      n2 = t[2]
      dl = v.tolist()
      rvs = U.empty_str()
      sp = U.space()
#
      max_n = 5
      n = 0
      break_out = False
      for i in range( n0 ):
         if break_out: break
         for j in range( n1 ):
            if break_out: break
            for k in range( n2 ):
               n += 1
               break_out = n > max_n
               if break_out: break
               xs = str( dl[i][j][k] )
               if n > 1: rvs += sp
               rvs += xs
# end of for k in range( n2 ): loop.
# end of for j in range( n1 ): loop.
# end of for i in range( n0 ): loop.
#
      return rvs
   # jl_dim3()
   def jl_the_data( vo ):
      v = vo[()]
      ndim = vo.ndim
      the_size = vo.size
      the_shape = vo.shape
      s = xs = \
      name_str = U.BN( vo.name )
      xs = xs.lower()
#      is_time = xs == 'time'
      is_time = 'time' in xs
      is_time = ( is_time ) and ( ndim == 1 ) and ( the_shape[0] == 1 )
#
      s = xs = U.empty_str()
      if ( the_size == 1 ) and ( ndim == 1 ): xs = jl_dim1_only1( v )
      if is_time: xs += U.time.asctime( U.time.gmtime( int( xs ) ) )
      if ( the_size > 1 ) and ( ndim == 1 ): xs = jl_dim1( v )
      if ndim == 2: xs = jl_dim2( v )
      if ndim == 3: xs = jl_dim3( v )
      s += xs
#
      return s
   # jl_the_data()

# fdo = first dataset object for the group.
   def report_group_attributes( fdo, indent ):
# access the value object so we can look at necessary data items.
      vo = fdo.value_object
# look at the parent to see the group attributes.
      the_parent = vo.parent
# see if there are attributes to report.
      we_have_attributes = U.object_has_attribute( the_parent, "attrs" )
      if we_have_attributes:
         pass
      else:
         print( "%sGroup Attributes:" %( indenter( indent ) ) )
         print( "%sNo Attributes Were Found For This Group!" %( indenter( indent + 3 ) ) )
         return None
# gather the attributes for reporting.
      ad = attrs_to_dict( the_parent.attrs )
# output the group attributes here.
      kl = ad.keys()
      num_attributes = len( kl )
#
      print( "%s%d Group %s:" \
      %( \
      indenter( indent ), \
      num_attributes, \
      U.plural( num_attributes, "attribute","attributes" ) \
      ) \
      )
#
      for ks in kl:
         vs = ad[ks]
         vs = jl_bytes( vs )
         print( "%s(%s) =" %( indenter( indent + 3 ), ks ) )
         vs = jl_edit( vs, indent + 6 )
         print( vs )
#         print( "(%s)" %( vs ) )
# end of for ks in kl: loop.
#
      return None
   # report_group_attributes()

# ds = dataset for which the information is reported.
   def report_dataset_attributes( ds, indent ):
# access the value object so we can look at necessary data items.
      vo = ds.value_object
# see if there are attributes to report.
      we_have_attributes = U.object_has_attribute( vo, "attrs" )
      if we_have_attributes:
         pass
      else:
         print( "%sDataset Attributes:" %( indenter( indent ) ) )
         print( "%sNo Attributes Were Found For This Dataset!" %( indenter( indent + 3 ) ) )
         return None
# gather the attributes for reporting.
      ad = attrs_to_dict( vo.attrs )
# output the dataset attributes here.
      kl = ad.keys()
      num_attributes = len( kl )
#
      print( "%s%d Dataset %s:" \
      %( \
      indenter( indent ), \
      num_attributes, \
      U.plural( num_attributes, "attribute","attributes" ) \
      ) \
      )
#
      for ks in kl:
         vs = ad[ks]
         vs = U.bytes_object_to_str_object( vs )
         print( "%s(%s) =" %( indenter( indent + 3 ), ks ) )
         print( "(%s)" %( vs ) )
# end of for ks in kl: loop.
#
      print()
#
      return None
   # report_dataset_attributes()
# start begin  generate_hdf5_hierarchy_report()
# leave now if the report is not desired.
   if not we_are_generating_the_report_file(): return False

   U.E()
   print( "Generating hdf5 hierarchy report for file:\n(%s)" %( hdf5_fn ) )

# a reference to simplify syntax.
   hgd = hdf5_groups_dictionary
   gl = hgd.keys() # gl = groups list.
   num_groups = len( gl )
   gc = 0 # gc = group counter

   indent = 0 # spaces to indent for print output.

# now process the groups in the hdf5 group dictionary
# to gain access to the list of the dataset information for each ggroup.
# gks = group key string
   for gks in gl:
      gc += 1 # count this group
      gns = gks # gns = group name string
      print( "%sGroup %d of %d (%s)" %( indenter(), gc, num_groups, gns ) )
# report the attributes for this group.
      first_dataset_object = hgd[gns][0] # get a reference to the ffirst dataset object in the group that is being processed.
      report_group_attributes( first_dataset_object, indent + 3 )
#
# now produce output for each hdf5 dataset in this hdf5 group.
      dsl = hgd[gns] # get a reference to the dataset list (dsl) for the group that is being processed.
      num_datasets = len( dsl )
      dc = 0 # dc = dataset counter
#
      print( "%s%d %s:" \
      %( \
      indenter( indent + 6), \
      num_datasets, \
      U.plural( num_datasets, "Dataset","Datasets" ) \
      ) \
      )
#
# now process the hdf5 datasets.
      for ds in dsl:
         dc += 1 # count this dataset
         print( "%sDataset %d of %d = (%s) (%s):" \
         %( \
         indenter( indent + 9), \
         dc, \
         num_datasets, \
         ds.my_name, \
         U.BN( ds.my_name ), \
         ) \
         )
# refer to the value object (vo) for the current dataset (ds).
         vo = ds.value_object
#
#         print("wwwwwwwww\n%s\nxxxxxxxxxxx" %( U.get_object_dir_str( vo )))
         indent_str = indenter( indent + 12 )
         print( """%s
%s
%s
%s
%s
""" \
         %( \
         indent_str + "Number Of Dimensions (ndim): " + str( vo.ndim ), \
  indent_str + "Shape: " + str( vo.shape ), \
         indent_str + "Size (number of items in data element): " + str( vo.size ), \
         indent_str + "Data Type: " + str( vo.dtype ), \
         indent_str + "Data: " + jl_the_data( vo ), \
         ) \
         )
#
         report_dataset_attributes( ds, indent + 12 )
#
# end of for ds in dsl: loop.
#
# end of for gks in gl: loop.

   U.L()

   return True
# generate_hdf5_hierarchy_report()


def load_ingest_configuration_dictionary_from_file(ingest_config_fn):

   global g_ingest_config_kl; # will log the order that the ingest config keys wered processed.
# leave early if generating the report file that will be used to create the config file loaded by this function.
   if we_are_generating_the_report_file(): return None

# ingest_config_d = the ingest dictionary that will return the ingest configuration commands for each dataset to the caller.
   ingest_config_d = dict()

# get the name of the ingest configuration file from the command line.
#   ingest_config_fn = U.get_cml_arg( "ingest_config_file", "-ingest_config_file NotOnCommandLine" )

# open the file for reading.
#   print("jjjj\n(%s)" %( ingest_config_fn))
   in_f = open( ingest_config_fn, "rt" )

# process the file.
   comment_str = "#"

   dataset_name = None

   while True:
      s = in_f.readline()
      if len( s ) == 0: break # end of file?      
      s = s.strip()
      find_ind = s.find( comment_str )
      line_has_comment = find_ind >= 0
      comment_at_start = find_ind == 0
      if line_has_comment: s = s[:find_ind]
      if comment_at_start: continue
      s = s.strip()
#
      new_dataset = s.startswith( "/" )
      if new_dataset:
         dataset_name = s
         g_ingest_config_kl.append( dataset_name ) # save the dataset name here for future processing order requirements.
         if dataset_name not in ingest_config_d: ingest_config_d[dataset_name] = list()
         ingest_config_d[dataset_name].append( U.empty_str() ) # populate the list with an empty string.
         continue
      else:
# only append config data if we have found a dataset name ("/Grid/precipitation")
# in the config file.
         if dataset_name == None: continue
         ingest_config_d[dataset_name].append( s )
#   
# end of while True: loop.

# close the input file.
   in_f.close()

# return the ingest dictionary to the caller.
   return ingest_config_d
# load_ingest_configuration_dictionary_from_file()


def get_initial_child_dictionary():
   return get_initial_pd_child_dictionary()
# get_initial_child_dictionary()


# a dataset dictionary is one that will be placed in either the
# GroupDictionary["coords"] or in the GroupDictionary["data_vars"] dictionaries.
def get_initial_dataset_dictionary():
   return get_initial_child_dictionary()
# get_initial_dataset_dictionary()


def get_data_list( vd ):
   return get_coords_child_data_list( vd )
# get_data_list()



def joe( d ):
   stay_here = U.is_cml_arg( "joe" )
   leave_now = not stay_here
#jl   print("bbbbb\n%s %s\ncccccccc" %( stay_here,leave_now))
   if leave_now: return None
   U.E()
   cd = d["coords"]
   cd_kl = list( cd.keys() )
   cd_kl.sort()
   dvd = d["data_vars"]
   dvd_kl = list( dvd.keys() )
   dvd_kl.sort()

   for ks in cd_kl:
      print("c: (%s)" %( ks ))
   for ks in dvd_kl:
      print("d: (%s)" %( ks ))


   print("d"*55 )
   print("cd type: %s" %( U.the_type( cd ) ) )
   print("dvd type: %s" %( U.the_type( dvd ) ) )
   print("e"*55 )

   for ks in cd_kl:
      xd = cd[ks]
      v = xd["data"]
      print("from cd (%s) data's type is (%s)" %( ks , U.the_type(v)))
      print("len of v: %d" %( len(v)) )
      print("v[0] = %d" %( v[0]) )
      print("end of (%s) info." %( ks ) )
# end of for ks in cd_kl: loop.

   for ks in dvd_kl:
      xd = dvd[ks]
      v = xd["data"]
      print("from dvd (%s) data's type is (%s)" %( ks , U.the_type(v)))
# end of for ks in cd_kl: loop.
   print("f"*55 )
   U.L()
   return None
# joe()


def we_are_generating_the_report_file():
   return U.is_cml_arg( "report" )
# we_are_generating_the_report_file()

# added by joseph.lang@noaa.gov on 09/02/2020
# to make config file code easier and self-documenting.
g_dim_d = dict() # global dimension dictionary.

def set_dim( reference_keys, the_name, the_value ):
   global g_dim_d;

# make sure the reference key information is in a list.
   is_list = U.is_a_list( reference_keys )
   is_tuple = U.is_a_tuple( reference_keys )

   if ( not is_list ) and ( not is_tuple ):
      rkl = [ reference_keys ] # rkl = reference keys list
   else:
      rkl = list( reference_keys )

# a dictionary in which to save the new name and associated value.
   new_d = dict()

# store the information.
   new_d["name"] = the_name

   new_d["value"] = the_value

# now save the new dictionary in gthe global dictionary for future reference.
# rks = reference key string.
   for rks in rkl:
      g_dim_d[rks] = new_d
# end of for rks in rkl: loop.

   return None
# set_dim()

def get_dim_d( the_key ):
   global g_dim_d;

   return g_dim_d[the_key]
# get_dim_d()

def get_dim_name( the_key ):
   d = get_dim_d( the_key )
   return d["name"]
# get_dim_name()


def get_dim_value( the_key ):
   d = get_dim_d( the_key )
   return d["value"]
# get_dim_value()
