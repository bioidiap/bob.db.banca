#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
# Laurent El Shafey <laurent.el-shafey@idiap.ch>
#
# Copyright (C) 2011-2013 Idiap Research Institute, Martigny, Switzerland
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""A few checks at the BANCA database.
"""

import os, sys
import unittest
import xbob.db.banca

class BancaDatabaseTest(unittest.TestCase):
  """Performs various tests on the BANCA database."""

  def test01_clients(self):
    # test whether the correct number of clients is returned
    db = xbob.db.banca.Database()

    self.assertEqual(len(db.clients()), 82)
    self.assertEqual(len(db.clients(groups='world')), 30)
    self.assertEqual(len(db.clients(groups='world', subworld='onethird')), 10)
    self.assertEqual(len(db.clients(groups='world', subworld='twothirds')), 20)
    self.assertEqual(len(db.clients(groups='dev')), 26)
    self.assertEqual(len(db.clients(groups='eval')), 26)
    self.assertEqual(len(db.tclients(groups='dev')), 26)
    self.assertEqual(len(db.tclients(groups='eval')), 26)

    self.assertEqual(len(db.clients(genders='f')), 41)
    self.assertEqual(len(db.clients(genders='m')), 41)


  def test02_objects(self):
    # tests if the right number of File objects is returned
    db = xbob.db.banca.Database()

    self.assertEqual(len(db.objects()), 6540)
    self.assertEqual(len(db.objects(groups='world')), 300)
    self.assertEqual(len(db.objects(groups='world', subworld='onethird')), 100)
    self.assertEqual(len(db.objects(groups='world', subworld='twothirds')), 200)
    self.assertEqual(len(db.objects(groups='dev')), 3120)
    self.assertEqual(len(db.objects(groups='eval')), 3120)

    # test for the different protocols
    for protocol in db.protocols():
      # assure that the number of enroll files is independent from the protocol
      for group in ('dev', 'eval'):
        self.assertEqual(len(db.objects(groups=group, purposes='enrol')), 390)
        for model_id in db.model_ids(groups=group):
          self.assertEqual(len(db.objects(groups=group, purposes='enrol', model_ids=model_id)), 15)
        for model_id in db.tmodel_ids(groups=group):
          self.assertEqual(len(db.tobjects(groups=group, model_ids=model_id)), 15)

      # check the number of probe files
      for group in ('dev', 'eval'):
        self.assertEqual(len(db.objects(groups=group, purposes='probe')), 2730)
        for model_id in db.model_ids(groups=group):
          self.assertEqual(len(db.objects(groups=group, purposes='probe', model_ids=model_id)), 105)
        for model_id in db.tmodel_ids(groups=group):
          self.assertEqual(len(db.zobjects(groups=group, model_ids=model_id)), 105)


  def test03_annotations(self):
    # Tests that for all files the annotated eye positions exist and are in correct order
    db = xbob.db.banca.Database()

    for f in db.objects():
      annotations = db.annotations(f.id)
      self.assertTrue(annotations is not None)
      self.assertEqual(len(annotations), 2)
      self.assertTrue('leye' in annotations)
      self.assertTrue('reye' in annotations)
      self.assertEqual(len(annotations['reye']), 2)
      self.assertEqual(len(annotations['leye']), 2)
      # assert that the eye positions are not exchanged
      self.assertTrue(annotations['leye'][1] > annotations['reye'][1])


  def test04_driver_api(self):

    from bob.db.script.dbmanage import main
    self.assertEqual(main('banca dumplist --self-test'.split()), 0)
    self.assertEqual(main('banca dumplist --protocol=P --class=client --group=dev --purpose=enrol --client=1008 --self-test'.split()), 0)
    self.assertEqual(main('banca checkfiles --self-test'.split()), 0)
    self.assertEqual(main('banca reverse 05/1021_f_g2_s05_1026_en_3 --self-test'.split()), 0)
    self.assertEqual(main('banca path 2327 --self-test'.split()), 0)

