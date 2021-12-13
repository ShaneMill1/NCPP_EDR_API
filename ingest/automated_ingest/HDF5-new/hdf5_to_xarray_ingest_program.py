"""
Program:
h2x_new_ingest_program.py
This program reads an hdf5 file,
harvests necessary data components from specified datasets,
and then produces xarray datasets from that data.

Author:
Joeph.Lang@noaa.gov

Created:
05/12/2020

Note:

pd in a object name indicate a "parent dictionary".
"""

import os;
import sys;
import time;

import h5py;
import xarray;

import hdf5_to_xarray_module as H2X;
import h2x_utils_module as U;

if U.python3():
   xrange = range
else:
   pa

def main():

   if __name__ != "__main__": return None

   ad = U.initialize_argument_dictionary( initialize_argument_dictionary )

   U.introduction( "None", "None" )

   U.E()

   print( U.get_command_line_argument_info_string() )

   output_h5py_version()

# Shane, new function invocation as of 06/23/2020.
   xarray_dataset = \
   H2X.get_xarray_dataset_from_hdf5_file( \
   the_hdf5_input_filename = ad["hdf5 input filename"], \
   )

   U.L()

   U.epilogue( ad["start time"] )

   return None
# main()


def initialize_argument_dictionary( ad ):

# put cript/program pecific initializations right here in this function.

# get the name of the hdf5 file from the command line where the input hdf5 data will be found.
   ad["hdf5 input filename"] = \
   x = \
   get_hdf5_input_filename_from_command_line()

# end the argument dictionary (ad) back to the caller.
   return ad
# initialize_argument_dictionary()


def get_hdf5_input_filename_from_command_line():

   big_comment = """
   shanes_nasa_data_dir = \
   '/home/smill/NASA_DATA/3IMERG'

   first_hdf5_file = '3B-MO.MS.MRG.3IMERG.20000601-S000000-E235959.06.V06B.HDF5'

   default_hdf5_file = os.path.join( shanes_nasa_data_dir, first_hdf5_file )
""" # end of big_comment

   cml_flag = "hdf5_input_filename"

   xs = U.get_cml_arg_flag_prefix_str() + \
   cml_flag + " Not Found On Command Line"

   default_hdf5_file = xs.replace( U.space(), "_" )

   x = U.get_cml_arg( \
   cml_flag, \
   default_hdf5_file \
   )

   rv = U.full_path( x )

   return rv
# get_hdf5_input_filename_from_command_line()

def output_h5py_version():
   print("Python version:" )
   pv = sys.version
   f= U.new_line()
   ind = pv.find( f )
   if ind > 0: pv = pv[:ind]
   print( pv )
   print("h5py version:" )
   print( h5py.__version__ )
   return None
# output_h5py_version()


def old_get_dataset_info_str( ad, d ):
   num_dataset = len( d["dataset names list"] )
   dataset_info_tr = \
   """
This file has %d %s.
""" \
   %( \
   num_dataset, \
   U.plural( num_dataset, "dataset", "datasets" ), \
   )

# a bit of editing.
   dataset_info_ltr = dataset_info_str.strip()

# here produce a string that will contain the info about each dataset.
   z = U.empty_tr()
   for xd in d["dataset object dictionary lit"]:
      x = "dataset: (%s) shape: [ %s ]  dtype: (%s)%s" \
      %( xd["dataset name"], xd["hape str"], xd["datatype str"], U.new_line() )
      z += x
# end of for xd in d["dataset object dictionary lit"]: loop.

# attach the info string.
   dataset_info_tr += zs


# return the info string to the caller.
   return dataset_info_tr
# old_get_dataset_info_str()


# the main execution logic of this script starts here.
if U.is_cml_arg( "both" ): sys.stdout = sys.stderr
main()
