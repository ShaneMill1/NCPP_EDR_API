#!/bin/bash
# File: example_h2x_report_script.sh
# Author: joseph.lang@noaa.gov
# Purpose: This is an example script that demonstrates how to execute
# our hdf5 ingest program in its report mode.
#
standard_output_file=./report_standard_output.txt
standard_error_file=./report_standard_error.txt
#
# The next variable specifies the python interpreter to use
# to run our python code.
# This is some release of python 3.
python_interpreter=/home/MDL/smill/miniconda3/envs/local_server/bin/python
#
# Our python program code file is next.
python_program=hdf5_to_xarray_ingest_program.py
#
# There are several command line argument flags that are used
# to properly execute our ingest program.
#
# -hdf5_input_filename The_name_of_the_input_hdf5_file_to_be_ingested
# Specifies the name of the hdf5 file to ingest for xarray conversion.
#
# -hdf5_group the_hdf5_group_in_hdf5_file_to_ingest
# Specifies the name of the group within the hdf5 file to be ingested.
#
# -ingest_config_file name_ingest_config_file
# Specifies the name of the configuration file that contains python-like statements
# that are executed by our ingest script to accomplish "special" ingest logic
# that are written to ingest special cases of hdf5 data.
#
# -report
# Specifies that the ingest program is to produce a report of the hdf5 file's hierarchy.
# The report is written to the standard output file.
# When this option is specified,
# no ingest functionality is performed.
# This report is used to aid in the production of the ingest configuration file.
#
# -print_dict
# Specifies that the ingest program should send dictionary initialization information to the standard output file.
# This information is very helpful in monitoring how well the ingest process is progressing.
#
# -both
# Specifies that both the standard output file data and
# the standard error output file data
# should be written to the same file.
# This option is very helpful during ingest program and configuration file development.
#
# -no_more_arguments \
# Just a "marker" that is printed to the standard output file by the ingest program
# to indicate the end of the command line arguments.
# This option has absolutely no logical function.
#
# If you have any questions about how to execute our hdf5 ingest program please feel free to contact the author at:
# joseph.lang@noaa.gov
#
# Now execute the ingest program in report mode.
$python_interpreter \
$python_program \
-hdf5_input_filename \
/home/smill/NASA_DATA/3IMERG/3B-MO.MS.MRG.3IMERG.20000601-S000000-E235959.06.V06B.HDF5 \
-hdf5_group \
'/' \
-report 'the program will send the hdf5 report to the standard output file.' \
-no_more_arguments \
> $standard_output_file \
 2>$standard_error_file
# That's it!
