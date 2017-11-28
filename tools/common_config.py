#!/usr/bin/python
# -*- coding: utf-8 -*-

from config_generate import *

# parameters :
# 1 - config file is      rozofs.conf under /etc/rozofs or /usr/local/etc/rozofs
# 2 - global variable is  common_config of type common_config_t define in coomon_config.h 
# 3 - rozodiag CLI is     cconf
#
config_generate("rozofs.conf","common_config","cconf")
