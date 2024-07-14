# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import warnings
import pandas as pd
import sys
from .dataframe_reader import DataframeReader
from ..util.perf_measure import annotate


class HDF5Reader(DataframeReader):
    @annotate("HDF5Reader.__init__")
    def __init__(self, filename):
        # TODO Remove Arguments when Python 2.7 support is dropped
        if sys.version_info[0] == 2:
            super(HDF5Reader, self).__init__(filename)
        else:
            super().__init__(filename)

    @annotate("GprofDotReader._read_dataframe_from_file")
    def _read_dataframe_from_file(self, **kwargs):
        df = None
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=Warning)
            df = pd.read_hdf(self.filename, **kwargs)
        return df
