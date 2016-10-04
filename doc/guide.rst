.. vim: set fileencoding=utf-8 :
.. @author: Manuel Guenther <Manuel.Guenther@idiap.ch>
.. @date:   Fri Oct 31 16:01:33 CET 2014

==============
 User's Guide
==============

This package contains the access API and descriptions for the BANCA_ English Database.
The actual raw data for the database should be downloaded from the original URL.
This API is only compatible with the images from the BANCA English database.

This package only contains the Bob_ accessor methods to use the DB directly from python, with our certified protocols.
The BANCA_ database interface provides several face verification protocols, i.e., ``'P'``, ``'G'``, ``'Mc'``, ``'Md'``, ``'Ma'``, ``'Ud'``, ``'Ua'``.
These protocols are defined in detail in [Bail2003]_.


.. [Bail2003] **Enrique Bailly-Bailliére and others**. *The BANCA Database and Evaluation Protocol*, Lecture Notes in Computer Science Vol. 2688, pages 625-638, 2003.


The Database Interface
----------------------

The :py:class:`bob.db.banca.Database` complies with the standard biometric verification database as described in `bob.db.base <bob.db.base>`.

.. todo::
   Explain the particularities of the :py:class:`bob.db.banca.Database` database.


.. _banca: http://www.ee.surrey.ac.uk/CVSSP/banca
.. _bob: https://www.idiap.ch/software/bob
