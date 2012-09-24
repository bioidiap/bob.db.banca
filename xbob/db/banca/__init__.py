#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
# Laurent El Shafey <laurent.el-shafey@idiap.ch>

"""The BANCA Database for face verification
"""

from .query import Database
from .models import Client, Subworld, File, Protocol, ProtocolPurpose

__all__ = dir()
