# Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
# This file is part of Rozofs.
#
# Rozofs is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation, version 2.
#
# Rozofs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

install(FILES rozofs.7 DESTINATION share/man/man7 COMPONENT client)
install(FILES rozofsmount.8 DESTINATION share/man/man8 COMPONENT client)
install(FILES rozofs.conf.5 DESTINATION share/man/man5 COMPONENT client)
install(FILES storaged.8 DESTINATION share/man/man8 COMPONENT storage)
install(FILES storage.conf.5 DESTINATION share/man/man5 COMPONENT storage)
install(FILES storage_rebuild.8 DESTINATION share/man/man8 COMPONENT storage)
install(FILES rozo_storage_mount.8 DESTINATION share/man/man8 COMPONENT storage)
install(FILES rozofs.conf.5 DESTINATION share/man/man5 COMPONENT storage)
install(FILES exportd.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES export.conf.5 DESTINATION share/man/man5 COMPONENT export)
install(FILES rozofs.conf.5 DESTINATION share/man/man5 COMPONENT export)
install(FILES rozo.1 DESTINATION share/man/man1 COMPONENT manager)
install(FILES geomgr.8 DESTINATION share/man/man8 COMPONENT geomgr)
install(FILES geocli.8 DESTINATION share/man/man8 COMPONENT geomgr)
install(FILES geomgr.conf.5 DESTINATION share/man/man5 COMPONENT geomgr)
install(FILES rozofs.conf.5 DESTINATION share/man/man5 COMPONENT geomgr)
install(FILES rozodiag.8 DESTINATION share/man/man8 COMPONENT rozodiag)
install(FILES rozo_status.py.8 DESTINATION share/man/man8 COMPONENT rozodiag)
install(FILES rozolauncher.8 DESTINATION share/man/man8 COMPONENT rozodiag)
install(FILES rozo_warnquota.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES rozo_setquota.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES rozo_quotaon.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES rozo_repquota.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES rozo_rebalance.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES rozo_rcmd_server.8 DESTINATION share/man/man8 COMPONENT export)
install(FILES rozo_trash.8 DESTINATION share/man/man8 COMPONENT client)
install(FILES rozo_trashd.8 DESTINATION share/man/man8 COMPONENT client)
install(FILES rozo_trashd.conf.5 DESTINATION share/man/man5 COMPONENT client)
install(FILES rozodump.8 DESTINATION share/man/man8 COMPONENT storage)


#
# add doxygen target
#
if(DOXYGEN)
    add_custom_target(doxygen "${DOXYGEN_EXECUTABLE}" "${CMAKE_CURRENT_BINARY_DIR}/doxyfile")
else(DOXYGEN)
    message(STATUS "doxygen not found.")
endif(DOXYGEN)


#
# RozoFS User's Guide target using Sphinx
#
if(SPHINX)
    add_custom_target(sphinx_guide_html "${SPHINX}" -b html "${CMAKE_CURRENT_SOURCE_DIR}/users_guide_sphinx/" "${CMAKE_CURRENT_BINARY_DIR}/users_guide_sphinx/html")
    add_custom_target(sphinx_guide_latex "${SPHINX}" -b latex "${CMAKE_CURRENT_SOURCE_DIR}/users_guide_sphinx/" "${CMAKE_CURRENT_BINARY_DIR}/users_guide_sphinx/latex")
    add_custom_target(sphinx_guide_pdf make -C "${CMAKE_CURRENT_BINARY_DIR}/users_guide_sphinx/latex" all-pdf)
    add_dependencies(sphinx_guide_pdf sphinx_guide_latex)
else(SPHINX)
    message(STATUS "sphinx-build not found.")
endif(SPHINX)

