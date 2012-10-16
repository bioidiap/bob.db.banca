#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
# Laurent El Shafey <Laurent.El-Shafey@idiap.ch>

"""This module provides the Dataset interface allowing the user to query the
BANCA database in the most obvious ways.
"""

import os
from bob.db import utils
from .models import *
from .driver import Interface

INFO = Interface()

SQLITE_FILE = INFO.files()[0]

class Database(object):
  """The dataset class opens and maintains a connection opened to the Database.

  It provides many different ways to probe for the characteristics of the data
  and for the data itself inside the database.
  """

  def __init__(self):
    # opens a session to the database - keep it open until the end
    self.connect()

  def connect(self):
    """Tries connecting or re-connecting to the database"""
    if not os.path.exists(SQLITE_FILE):
      self.session = None

    else:
      self.session = utils.session_try_readonly(INFO.type(), SQLITE_FILE)

  def is_valid(self):
    """Returns if a valid session has been opened for reading the database"""

    return self.session is not None

  def assert_validity(self):
    """Raise a RuntimeError if the database backend is not available"""

    if not self.is_valid():
      raise RuntimeError, "Database '%s' cannot be found at expected location '%s'. Create it and then try re-connecting using Database.connect()" % (INFO.name(), SQLITE_FILE)

  def __group_replace_alias__(self, l):
    """Replace 'dev' by 'g1' and 'eval' by 'g2' in a list of groups, and
       returns the new list"""
    if not l: return l
    elif isinstance(l, str): return self.__group_replace_alias__((l,))
    l2 = []
    for val in l:
      if(val == 'dev'): l2.append('g1')
      elif(val == 'eval'): l2.append('g2')
      else: l2.append(val)
    return tuple(l2)

  def __check_validity__(self, l, obj, valid, default):
    """Checks validity of user input data against a set of valid values"""
    if not l: return default
    elif not isinstance(l, (tuple,list)):
      return self.__check_validity__((l,), obj, valid, default)
    for k in l:
      if k not in valid:
        raise RuntimeError, 'Invalid %s "%s". Valid values are %s, or lists/tuples of those' % (obj, k, valid)
    return l

  def groups(self):
    """Returns the names of all registered groups"""

    return ProtocolPurpose.group_choices

  def client_groups(self):
    """Returns the names of the XM2VTS groups. This is specific to this database which
    does not have separate training, development and evaluation sets."""

    return Client.group_choices

  def genders(self):
    """Returns the list of genders: 'm' for male and 'f' for female"""

    return Client.gender_choices

  def languages(self):
    """Returns the list of languages"""

    return Client.language_choices

  def subworld_names(self):
    """Returns all registered subworld names"""

    self.assert_validity()
    l = self.subworlds()
    retval = [str(k.name) for k in l]
    return retval

  def subworlds(self):
    """Returns the list of subworlds"""

    self.assert_validity()

    return list(self.session.query(Subworld))

  def has_subworld(self, name):
    """Tells if a certain subworld is available"""

    self.assert_validity()
    return self.session.query(Subworld).filter(Subworld.name==name).count() != 0

  def clients(self, protocol=None, groups=None, gender=None, language=None, subworld=None):
    """Returns a set of clients for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    groups
      The groups to which the clients belong ('g1', 'g2', 'world').
      Note that 'dev' is an alias to 'g1' and 'eval' an alias to 'g2'

    gender
      The gender to which the clients belong ('f', 'm')

    language
      TODO: only English is currently supported
      The language spoken by the clients ('en',)

    subworld
      Specify a split of the world data ('onethird', 'twothirds')
      In order to be considered, 'world' should be in groups and only one
      split should be specified.

    Returns: A list containing all the clients which have the given properties.
    """

    self.assert_validity()

    groups = self.__group_replace_alias__(groups)
    VALID_GROUPS = self.client_groups()
    VALID_GENDERS = self.genders()
    VALID_LANGUAGES = self.languages()
    VALID_SUBWORLDS = self.subworld_names()
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, VALID_GROUPS)
    gender = self.__check_validity__(gender, "gender", VALID_GENDERS, VALID_GENDERS)
    language = self.__check_validity__(language, "language", VALID_LANGUAGES, VALID_LANGUAGES)
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, VALID_SUBWORLDS)

    retval = []
    # List of the clients
    if "world" in groups:
      if len(subworld)==1:
        q = self.session.query(Client).join(Subworld).filter(Subworld.name.in_(subworld))
      else:
        q = self.session.query(Client).filter(Client.sgroup == 'world')
      q = q.filter(Client.gender.in_(gender)).\
            filter(Client.language.in_(language)).\
          order_by(Client.id)
      retval += list(q)

    if 'g1' in groups or 'g2' in groups:
      q = self.session.query(Client).filter(Client.sgroup != 'world').\
            filter(Client.sgroup.in_(groups)).\
            filter(Client.gender.in_(gender)).\
            filter(Client.language.in_(language)).\
            order_by(Client.id)
      retval += list(q)

    return retval

  def tclients(self, protocol=None, groups=None):
    """Returns a set of T-Norm clients for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    groups
      The groups to which the clients belong ('g1', 'g2').
      Note that 'dev' is an alias to 'g1' and 'eval' an alias to 'g2'

    Returns: A list containing all the T-norm clients which have the given properties.
    """

    groups = self.__group_replace_alias__(groups)
    VALID_GROUPS = ('g1', 'g2')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, VALID_GROUPS)
    # g2 clients are used for normalizing g1 ones, etc.
    tgroups = []
    if 'g1' in groups:
      tgroups.append('g2')
    if 'g2' in groups:
      tgroups.append('g1')
    return self.clients(protocol, tgroups)

  def zclients(self, protocol=None, groups=None):
    """Returns a set of Z-Norm clients for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    groups
      The groups to which the clients belong ('g1', 'g2').
      Note that 'dev' is an alias to 'g1' and 'eval' an alias to 'g2'

    Returns: A list containing all the Z-norm clients which have the given properties.
    """

    groups = self.__group_replace_alias__(groups)
    VALID_GROUPS = ('g1', 'g2')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS)
    # g2 clients are used for normalizing g1 ones, etc.
    zgroups = []
    if 'g1' in groups:
      zgroups.append('g2')
    if 'g2' in groups:
      zgroups.append('g1')
    return self.clients(protocol, zgroups)


  def models(self, protocol=None, groups=None):
    """Returns a set of models for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    groups
      The groups to which the subjects attached to the models belong ('g1', 'g2', 'world')
      Note that 'dev' is an alias to 'g1' and 'eval' an alias to 'g2'

    Returns: A list containing all the models which have the given properties.
    """

    return self.clients(protocol, groups)

  def tmodels(self, protocol=None, groups=None):
    """Returns a set of T-Norm models for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    groups
      The groups to which the clients belong ('g1', 'g2').
      Note that 'dev' is an alias to 'g1' and 'eval' an alias to 'g2'

    Returns: A list containing all the Z-norm models which have the given properties.
    """

    return self.tclients(protocol, groups)

  def has_client_id(self, id):
    """Returns True if we have a client with a certain integer identifier"""

    self.assert_validity()
    return self.session.query(Client).filter(Client.id==id).count() != 0

  def client(self, id):
    """Returns the client object in the database given a certain id. Raises
    an error if that does not exist."""

    self.assert_validity()
    return self.session.query(Client).filter(Client.id==id).one()

  def get_client_id_from_model_id(self, model_id):
    """Returns the client_id attached to the given model_id

    Keyword Parameters:

    model_id
      The model_id to consider

    Returns: The client_id attached to the given model_id
    """
    return model_id

  def get_client_id_from_tmodel_id(self, tmodel_id):
    """Returns the client_id attached to the given T-Norm model_id

    Keyword Parameters:

    tmodel_id
      The tmodel_id to consider

    Returns: The client_id attached to the given T-Norm model_id
    """
    return tmodel_id

  def objects(self, protocol=None, purposes=None, model_ids=None, groups=None,
      classes=None, languages=None, subworld=None):
    """Returns a set of Files for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    purposes
      The purposes required to be retrieved ('enrol', 'probe', 'train') or a tuple
      with several of them. If 'None' is given (this is the default), it is
      considered the same as a tuple with all possible values. This field is
      ignored for the data from the "world" group.

    model_ids
      Only retrieves the files for the provided list of model ids (claimed
      client id).  If 'None' is given (this is the default), no filter over
      the model_ids is performed.

    groups
      One of the groups ('dev', 'eval', 'world') or a tuple with several of them.
      If 'None' is given (this is the default), it is considered the same as a
      tuple with all possible values.

    classes
      The classes (types of accesses) to be retrieved ('client', 'impostor')
      or a tuple with several of them. If 'None' is given (this is the
      default), it is considered the same as a tuple with all possible values.

    languages
      The language spoken by the clients ('en')
      TODO: only English is currently supported
      If 'None' is given (this is the default), it is considered the same as a
      tuple with all possible values.

    subworld
      Specify a split of the world data ('onethird', 'twothirds')
      In order to be considered, 'world' should be in groups and only one
      split should be specified.

    Returns: A list of files which have the given properties.
    """

    self.assert_validity()

    VALID_PROTOCOLS = self.protocol_names()
    VALID_PURPOSES = self.purposes()
    VALID_GROUPS = self.groups()
    VALID_LANGUAGES = self.languages()
    VALID_CLASSES = ('client', 'impostor')
    VALID_SUBWORLDS = self.subworld_names()

    protocol = self.__check_validity__(protocol, "protocol", VALID_PROTOCOLS, VALID_PROTOCOLS)
    purposes = self.__check_validity__(purposes, "purpose", VALID_PURPOSES, VALID_PURPOSES)
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, VALID_GROUPS)
    languages = self.__check_validity__(languages, "language", VALID_LANGUAGES, VALID_LANGUAGES)
    classes = self.__check_validity__(classes, "class", VALID_CLASSES, VALID_CLASSES)
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, VALID_SUBWORLDS)

    import collections
    if(model_ids is None):
      model_ids = ()
    elif(not isinstance(model_ids,collections.Iterable)):
      model_ids = (model_ids,)

    # Now query the database
    retval = []
    if 'world' in groups:
      q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocolPurposes).join(Protocol)
      if len(subworld) == 1:
        q = q.join(Subworld).filter(Subworld.name.in_(subworld))
      q = q.filter(Client.sgroup == 'world').\
            filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup == 'world')).\
            filter(Client.language.in_(languages))
      if model_ids:
        q = q.filter(Client.id.in_(model_ids))
      q = q.order_by(File.client_id, File.session_id, File.claimed_id, File.shot_id)
      retval += list(q)

    if ('dev' in groups or 'eval' in groups):
      if('enrol' in purposes):
        q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocolPurposes).join(Protocol).\
              filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup.in_(groups), ProtocolPurpose.purpose == 'enrol'))
        if model_ids:
          q = q.filter(Client.id.in_(model_ids))
        q = q.order_by(File.client_id, File.session_id, File.claimed_id, File.shot_id)
        retval += list(q)

      if('probe' in purposes):
        if('client' in classes):
          q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocolPurposes).join(Protocol).\
                filter(File.client_id == File.claimed_id).\
                filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup.in_(groups), ProtocolPurpose.purpose == 'probe'))
          if model_ids:
            q = q.filter(Client.id.in_(model_ids))
          q = q.order_by(File.client_id, File.session_id, File.claimed_id, File.shot_id)
          retval += list(q)

        if('impostor' in classes):
          q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocolPurposes).join(Protocol).\
                filter(File.client_id != File.claimed_id).\
                filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup.in_(groups), ProtocolPurpose.purpose == 'probe'))
          if model_ids:
            q = q.filter(File.claimed_id.in_(model_ids))
          q = q.order_by(File.client_id, File.session_id, File.claimed_id, File.shot_id)
          retval += list(q)

    return list(set(retval)) # To remove duplicates

  def tobjects(self, protocol=None, model_ids=None, groups=None, languages=None):
    """Returns a set of Files for enrolling T-norm models for score
       normalization.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    model_ids
      Only retrieves the files for the provided list of model ids (claimed
      client id).  If 'None' is given (this is the default), no filter over
      the model_ids is performed.

    groups
      The groups to which the clients belong ('dev', 'eval').

    languages
      The language spoken by the clients ('en')
      TODO: only English is currently supported
      If 'None' is given (this is the default), it is considered the same as a
      tuple with all possible values.

    Returns: A list of Files which have the given properties.
    """

    VALID_GROUPS = ('dev', 'eval')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, VALID_GROUPS)
    # g2 clients are used for normalizing g1 ones, etc.
    tgroups = []
    if 'dev' in groups:
      tgroups.append('eval')
    if 'eval' in groups:
      tgroups.append('dev')
    return self.objects(protocol, 'enrol', model_ids, tgroups, 'client', languages)

  def zobjects(self, protocol=None, model_ids=None, groups=None, languages=None):
    """Returns a set of Files to perform Z-norm score normalization.

    Keyword Parameters:

    protocol
      One of the BANCA protocols ('P', 'G', 'Mc', 'Md', 'Ma', 'Ud', 'Ua').

    model_ids
      Only retrieves the files for the provided list of model ids (claimed
      client id).  If 'None' is given (this is the default), no filter over
      the model_ids is performed.

    groups
      The groups to which the clients belong ('dev', 'eval').

    languages
      The language spoken by the clients ('en')
      TODO: only English is currently supported
      If 'None' is given (this is the default), it is considered the same as a
      tuple with all possible values.

    Returns: A list of Files which have the given properties.
    """

    VALID_GROUPS = ('dev', 'eval')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, VALID_GROUPS)
    # g2 clients are used for normalizing g1 ones, etc.
    zgroups = []
    if 'dev' in groups:
      zgroups.append('eval')
    if 'eval' in groups:
      zgroups.append('dev')
    return self.objects(protocol, 'probe', model_ids, zgroups, None, languages)

  def protocol_names(self):
    """Returns all registered protocol names"""

    self.assert_validity()
    l = self.protocols()
    retval = [str(k.name) for k in l]
    return retval

  def protocols(self):
    """Returns all registered protocols"""

    self.assert_validity()
    return list(self.session.query(Protocol))

  def has_protocol(self, name):
    """Tells if a certain protocol is available"""

    self.assert_validity()
    return self.session.query(Protocol).filter(Protocol.name==name).count() != 0

  def protocol(self, name):
    """Returns the protocol object in the database given a certain name. Raises
    an error if that does not exist."""

    self.assert_validity()
    return self.session.query(Protocol).filter(Protocol.name==name).one()

  def protocol_purposes(self):
    """Returns all registered protocol purposes"""

    self.assert_validity()
    return list(self.session.query(ProtocolPurpose))

  def purposes(self):
    """Returns the list of allowed purposes"""

    return ProtocolPurpose.purpose_choices

  def paths(self, ids, prefix='', suffix=''):
    """Returns a full file paths considering particular file ids, a given
    directory and an extension

    Keyword Parameters:

    id
      The ids of the object in the database table "file". This object should be
      a python iterable (such as a tuple or list).

    prefix
      The bit of path to be prepended to the filename stem

    suffix
      The extension determines the suffix that will be appended to the filename
      stem.

    Returns a list (that may be empty) of the fully constructed paths given the
    file ids.
    """

    self.assert_validity()

    fobj = self.session.query(File).filter(File.id.in_(ids))
    retval = []
    for p in ids:
      retval.extend([k.make_path(prefix, suffix) for k in fobj if k.id == p])
    return retval

  def reverse(self, paths):
    """Reverses the lookup: from certain stems, returning file ids

    Keyword Parameters:

    paths
      The filename stems I'll query for. This object should be a python
      iterable (such as a tuple or list)

    Returns a list (that may be empty).
    """

    self.assert_validity()

    fobj = self.session.query(File).filter(File.path.in_(paths))
    for p in paths:
      retval.extend([k.id for k in fobj if k.path == p])
    return retval

