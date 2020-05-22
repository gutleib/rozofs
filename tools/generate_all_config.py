#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
from config_generate import *

#_______________________________________________________________________________
#
# config_generate : Generate code for one configuration file
#
# @param file_name           The config file name used on site by RozoFS software 
#                            to save the configuration.
#
# @param input_file_name     relative path from tool directory of input file 
#                            (with .input suffix) describing the configuration
#                            parameters (type, usage and possible values).
#
# @param cli_name            The CLI name to register in rozodiag interface
#                            for managing the configuration.
#_______________________________________________________________________________


config_generate(file_name       = "rozofs.conf",     
                input_file_name = "../rozofs/common/common_config.input",   
                cli_name        = "cconf")


config_generate(file_name       = "rebalance.conf",     
                input_file_name = "../src/exportd/rebalance_config.input",   
                 cli_name       = "rebalanceconf")
 
                                                
config_generate(file_name       = "trash.conf",     
                input_file_name = "../src/exportd/trash_process_config.input",   
                cli_name        = "trash_process_config")
                                                
config_generate(file_name       = "rozofsmount_netdata_cfg.conf",     
                input_file_name = "../src/rozofsmount/rozofsmount_netdata_cfg.input",   
                cli_name        = "netdata")
                                                
