# Copyright 2017-2019 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import subprocess
import numpy as np

import pytest

from hatchet import GraphFrame
from hatchet.readers.caliper_reader import CaliperReader
from hatchet.util.executable import which

annotations = [
    "main",
    "LagrangeLeapFrog",
    "LagrangeElements",
    "ApplyMaterialPropertiesForElems",
    "EvalEOSForElems",
    "CalcEnergyForElems",
    "CalcPressureForElems",
    "CalcSoundSpeedForElems",
    "UpdateVolumesForElems",
    "CalcTimeConstraintsForElems",
    "CalcCourantConstraintForElems",
    "CalcHydroConstraintForElems",
    "TimeIncrement",
    "LagrangeNodal",
    "CalcForceForNodes",
    "CalcVolumeForceForElems",
    "IntegrateStressForElems",
    "CalcHourglassControlForElems",
    "CalcFBHourglassForceForElems",
    "CalcLagrangeElements",
    "CalcKinematicsForElems",
    "CalcQForElems",
    "CalcMonotonicQGradientsForElems",
    "CalcMonotonicQRegionForElems",
]


def test_graphframe(lulesh_caliper_json):
    """Sanity test a GraphFrame object with known data."""
    gf = GraphFrame.from_caliper_json(str(lulesh_caliper_json))

    assert len(gf.dataframe.groupby("name")) == 24

    for col in gf.dataframe.columns:
        if col in ("time (inc)", "time"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("nid", "rank"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "node"):
            assert gf.dataframe[col].dtype == np.object

    # TODO: add tests to confirm values in dataframe


def test_read_calc_pi_database(lulesh_caliper_json):
    """Sanity check the Caliper reader by examining a known input."""
    reader = CaliperReader(str(lulesh_caliper_json))
    reader.read_json_sections()

    assert len(reader.json_data) == 192
    assert len(reader.json_cols) == 4
    assert len(reader.json_cols_mdata) == 4
    assert len(reader.json_nodes) == 24

    reader.create_graph()
    assert all(an in reader.idx_to_label.values() for an in annotations)


@pytest.mark.skipif(not which("cali-query"), reason="needs cali-query to be in path")
def test_sample_cali(sample_caliper_raw_cali):
    """Sanity check the Caliper reader ingesting a .cali file."""
    grouping_attribute = "function"
    default_metric = "sum(sum#time.duration),inclusive_sum(sum#time.duration)"
    query = "select function,%s group by %s format json-split" % (
        default_metric,
        grouping_attribute,
    )

    gf = GraphFrame.from_caliper(str(sample_caliper_raw_cali), query)

    assert len(gf.dataframe.groupby("name")) == 18


@pytest.mark.skipif(not which("cali-query"), reason="needs cali-query to be in path")
def test_json_string_literal(sample_caliper_raw_cali):
    """Sanity check the Caliper reader ingesting a JSON string literal."""
    cali_query = which("cali-query")
    grouping_attribute = "function"
    default_metric = "sum(sum#time.duration),inclusive_sum(sum#time.duration)"
    query = "select function,%s group by %s format json-split" % (
        default_metric,
        grouping_attribute,
    )

    cali_json = subprocess.Popen(
        [cali_query, "-q", query, sample_caliper_raw_cali], stdout=subprocess.PIPE
    )

    gf = GraphFrame.from_caliper_json(cali_json.stdout)

    assert len(gf.dataframe.groupby("name")) == 18


def test_sample_json(sample_caliper_json):
    """Sanity check the Caliper reader ingesting a JSON string literal."""
    gf = GraphFrame.from_caliper_json(str(sample_caliper_json))

    assert len(gf.dataframe.groupby("name")) == 18


def test_filter_squash_unify_caliper_data(lulesh_caliper_json):
    """Sanity test a GraphFrame object with known data."""
    gf1 = GraphFrame.from_caliper_json(str(lulesh_caliper_json))
    gf2 = GraphFrame.from_caliper_json(str(lulesh_caliper_json))

    assert gf1.graph is not gf2.graph
    with pytest.raises(ValueError):
        # this is an invalid comparison because the indexes are different at
        # this point
        gf1.dataframe["node"].apply(id) != gf2.dataframe["node"].apply(id)
    assert all(gf1.dataframe.index != gf2.dataframe.index)

    filter_gf1 = gf1.filter(lambda x: x["name"].startswith("Calc"))
    filter_gf2 = gf2.filter(lambda x: x["name"].startswith("Calc"))

    squash_gf1 = filter_gf1.squash()
    squash_gf2 = filter_gf2.squash()

    squash_gf1.unify(squash_gf2)

    # Indexes are now the same. Sort indexes before comparing.
    squash_gf1.dataframe.sort_index(inplace=True)
    squash_gf2.dataframe.sort_index(inplace=True)
    assert squash_gf1.graph is squash_gf2.graph
    assert all(
        squash_gf1.dataframe["node"].apply(id) == squash_gf2.dataframe["node"].apply(id)
    )

    assert all(squash_gf1.dataframe.index == squash_gf2.dataframe.index)
