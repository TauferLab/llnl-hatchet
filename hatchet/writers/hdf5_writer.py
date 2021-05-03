# Copyright 2017-2020 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import warnings

from .pandas_writer import PandasWriter


class HDF5Writer(PandasWriter):
    def __init__(self, filename):
        # TODO Remove Arguments when Python 2.7 support is dropped
        super(HDF5Writer, self).__init__(filename)

    def _write_to_file_type(self, df, **kwargs):
        if "key" not in kwargs:
            raise KeyError("Writing to HDF5 requires a user-supplied key")
        key = kwargs["key"]
        del kwargs["key"]
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=Warning)
            df.to_hdf(self.fname, key, **kwargs)
