from src.plugins.base_plugin import BasePlugin
from typing import List, Tuple

import numpy
from scipy.stats import linregress
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

class StatPlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        # self.__spark.udf.register("hrc", StatFunctions.hrc, StatFunctions.schema_s)
        # self.__spark.udf.register("pc", StatFunctions.pc, StatFunctions.schema_d)
        # self.__spark.udf.register("sf", StatFunctions.stat_feature, StatFunctions.schema_sf)
        # self.__spark.udf.register("rel", StatFunctions.relations, StatFunctions.schema_rel)
        # self.__spark.udf.register("lr", StatFunctions.lr, StatFunctions.schema_r)

    def register_udfs(self) -> None:
        super().register_udf("hrc", StatPlugin.hrc, StatPlugin.schema_s)
        super().register_udf("pc", StatPlugin.pc, StatPlugin.schema_d)
        super().register_udf("sf", StatPlugin.stat_feature, StatPlugin.schema_sf)
        super().register_udf("rel", StatPlugin.relations, StatPlugin.schema_rel)
        super().register_udf("lr", StatPlugin.lr, StatPlugin.schema_r)

    schema_s = StructType(
        [
            StructField("str_rep", StringType(), False),
            StructField("type", StringType(), False),
            StructField("start_year", IntegerType(), False),
            StructField("end_year", IntegerType(), False),
            StructField("result", FloatType(), False),
        ]
    )

    """Return type for calculations on time intervals of two words."""
    schema_d = StructType(
        [
            StructField("str_rep_1", StringType(), False),
            StructField("type_1", StringType(), False),
            StructField("str_rep_2", StringType(), False),
            StructField("type_2", StringType(), False),
            StructField("start_year", IntegerType(), False),
            StructField("end_year", IntegerType(), False),
            StructField("result", FloatType(), False),
        ]
    )

    """Return type for calculations of statistical features of a word."""
    schema_sf = StructType(
        [
            StructField("str_rep", StringType(), False),
            StructField("type", StringType(), False),
            StructField("mean", FloatType(), False),
            StructField("median", FloatType(), False),
            StructField("var", FloatType(), False),
            StructField("min", IntegerType(), False),
            StructField("max", IntegerType(), False),
            StructField("q_25", FloatType(), False),
            StructField("q_75", FloatType(), False),
            StructField("hrc", FloatType(), False),
        ]
    )

    """Return type for calculations of relations between two words."""
    schema_rel = StructType(
        [
            StructField("str_rep1", StringType(), False),
            StructField("type1", StringType(), False),
            StructField("str_rep2", StringType(), False),
            StructField("type2", StringType(), False),
            StructField("hrc_year", IntegerType(), False),
            StructField("hrc_max", FloatType(), False),
            StructField("cov", FloatType(), False),
            StructField("spearman_corr", FloatType(), False),
            StructField("pearson_corr", FloatType(), False),
        ]
    )

    """ Returns type for calculations of a linear regression given a time series """
    schema_r = StructType(
        [
            StructField("type", StringType(), False),
            StructField("slope", FloatType(), False),
            StructField("intercept", FloatType(), False),
            StructField("r_value", FloatType(), False),
            StructField("p_value", FloatType(), False),
            StructField("std_err", FloatType(), False),
        ]
    )

    @staticmethod
    def _rm_direction(rel_change: float) -> float:
        return rel_change if rel_change >= 0 else abs(1 / rel_change)

    @staticmethod
    def hrc(duration, word, w_type, *years):
        """Returns the strongest relative change between any two years that duration years apart."""
        """Examples: no change = 0, doubled = 1, halved -0.5"""
        """Example usage: select hrc['str_rep'] word, hrc['type'] type, hrc['start_year'] start, 
        hrc['end_year'] end, hrc['result'] hrc from (select hrc(3, *) hrc from schema_f)"""

        # F-tuple format: str_rep, type, frq_1800, ..., frq_2000
        y_offset: int = 1800
        year_count: int = 201  # 1800 -> 2000: 201

        # TODO: remove debugging code before submission
        debug = False

        hrc_result = 0.0
        result_start_year = 0
        result_end_year = 0
        duration = int(duration)

        if debug:
            print(f"duration: {duration}")
            print(f"word: {word}")
            print(f"type: {w_type}")
            print("years: ", years[:20])

        for year in range(0, (year_count - duration)):

            start: int = int(years[year])
            end: int = int(years[year + duration])

            # relative change for start value 0 does not exist
            if start == 0:
                continue

            change = (end - start) / start

            if StatPlugin._rm_direction(change) > StatPlugin._rm_direction(
                hrc_result
            ):
                hrc_result = change
                result_start_year = year + y_offset
                result_end_year = year + duration + y_offset

            if debug and year < 13:
                print(f"{y_offset + year} to {y_offset + year + duration}: {change}")

        # TODO: how to treat null values? for now set to empty string
        if not w_type:
            w_type = ""

        return word, w_type, result_start_year, result_end_year, hrc_result

    @staticmethod
    def pc(start_year, end_year, *fxf_tuple):
        """Returns the Pearson correlation coefficient of two time series
        (limited to the time period of [start year, end year])."""
        """Example usage: select pc['str_rep_1'] word_1, pc['type_1'] type_1, 
        pc['str_rep_2'] word_2, pc['type_2'] type_2, pc['start_year'] start, pc['end_year'] end, 
        pc['result'] pearson_corr from (select pc(1990, 2000, *) pc 
        from schema_f a cross join schema_f b where a.str_rep != b.str_rep)"""

        # FxF format: w1, t1, frq1_1800, ..., frq1_2000, w2, t2, frq2_1800, ..., frq2_2000
        y_offset: int = 1800
        year_count: int = 201  # 1800 -> 2000: 201

        # TODO: remove debugging code before submission
        debug = False

        start_year: int = int(start_year)
        end_year: int = int(end_year)

        # split input tuple
        word_1 = fxf_tuple[0]
        type_1 = fxf_tuple[1]
        freq_1 = fxf_tuple[2 : 2 + year_count]

        word_2 = fxf_tuple[(2 + year_count)]
        type_2 = fxf_tuple[(2 + year_count + 1)]
        freq_2 = fxf_tuple[(2 + year_count + 2) : (2 + year_count + 2 + year_count)]

        if debug:
            print(f"1: {word_1}_{type_1}  2: {word_2}_{type_2};")
            print("freq_1:", freq_1)
            print("freq_2:", freq_2)

        # limit to interval between start and end year, each inclusive
        start_index = start_year - y_offset
        end_index = end_year - y_offset + 1  # end year inclusive
        freq_1 = freq_1[start_index:end_index]
        freq_2 = freq_2[start_index:end_index]

        # TODO: handle case where pc does not exist, e.g. all entries of one interval are 0
        #       Also if all entries are 1 it might cause division by zero due to pc formular

        # pearson correlation coefficient in second entry in first row in matrix from numpy
        pearson_corr: float = float(numpy.corrcoef(freq_1, freq_2)[0][1])

        if debug:
            print("interval 1:", freq_1)
            print("interval 2:", freq_2)
            print("Pearson correlation:", pearson_corr)
            print(type(word_1), sep=", ")
            print(type(type_1), sep=", ")
            print(type(word_2), sep=", ")
            print(type(type_2), sep=", ")
            print(type(start_year), sep=", ")
            print(type(end_year), sep=", ")
            print(type(pearson_corr))

        # TODO: how to treat null values? for now set to empty string
        if not type_1:
            type_1 = ""
        if not type_2:
            type_2 = ""

        if debug:
            print(type(word_1), type(word_2), type(type_1), type(type_2), type(start_year), type(end_year), type(pearson_corr))

        return word_1, type_1, word_2, type_2, start_year, end_year, pearson_corr

    @staticmethod
    def stat_feature(str_rep, type, *years):
        """Returns statistical features from schema f."""
        """
        Example usage: 
        select sf.str_rep, sf.type, sf.mean, sf.median, sf.q_25, sf.q_75, sf.var, sf.min, sf.max, sf.hrc 
        from (select sf(*) sf from schema_f)
        """

        # F format: str_rep, type, frq_1800, ..., frq_2000

        if not type:
            type = ""

        f_int_list = [int(i) for i in years]
        f_array = numpy.asarray(f_int_list)

        # calculate statistical features
        mean = numpy.mean(f_array).item()
        median = numpy.median(f_array).item()
        q_25 = numpy.percentile(f_array, 25).item()
        q_75 = numpy.percentile(f_array, 75).item()
        var = numpy.var(f_array).item()
        min = numpy.amin(f_array).item()
        max = numpy.amax(f_array).item()
        hrc = StatPlugin.hrc(1, str_rep, type, *years)[-1]

        return str_rep, type, mean, median, var, min, max, q_25, q_75, hrc

    @staticmethod
    def relations(*fxf_tuple):
        """Returns relations between time series from schema fxf."""
        """
        Example usage: 
        select rel.str_rep1, rel.type1, rel.str_rep2, rel.type2, rel.hrc_year, 
        rel.hrc_max, rel.cov, rel.spearman_corr, rel.pearson_corr
        from (
            select rel(*) rel 
            from schema_f a cross join schema_f b where a.str_rep != b.str_rep
        )
        """

        # FxF format: w1, t1, frq1_1800, ..., frq1_2000, w2, t2, frq2_1800, ..., frq2_2000

        # split input tuple
        str_rep1 = fxf_tuple[0]
        type1 = fxf_tuple[1]
        freq1 = fxf_tuple[2:203]
        str_rep2 = fxf_tuple[203]
        type2 = fxf_tuple[204]
        freq2 = fxf_tuple[205:]

        if not type1:
            type1 = ""
        if not type2:
            type2 = ""

        # calculate 
        numpy.seterr(divide='ignore', invalid='ignore')
        hrc_l = numpy.divide(numpy.subtract(freq2, freq1), freq1)
        hrc_l = numpy.nan_to_num(numpy.absolute(hrc_l), nan=0.0, posinf=0.0, neginf=0.0)
        hrc_year = numpy.argmax(hrc_l).item() + 1800
        hrc_max = numpy.amax(hrc_l).item()
        cov = numpy.cov(freq1, freq2)[0][1].item()
        pearson_corr = StatPlugin.pc(1800, 2000, *fxf_tuple)[-1]
        spearman_corr = cov / (numpy.std(freq1).item() * numpy.std(freq2).item())

        return  str_rep1, type1, str_rep2, type2, hrc_year, hrc_max, cov, spearman_corr, pearson_corr

    @staticmethod
    def lr(*f_tuple) -> Tuple[str, float, float, float, float, float]:
        """Returns the linear regression coefficient of a time series."""
        """ Example usage: select lr(*) lr from (select * from schema_f limit 1)"""
        # remove data type from tuple
        tp = f_tuple[0]
        if tp is None:
            tp = 'None'
        f_tuple = f_tuple[2:]

        # F-tuple format: frq_1800, ..., frq_2000
        # generate years from 1800 to 2000
        years = range(1800, 2001)

        # convert to numpy array
        years = numpy.array(years)
        freq = numpy.array(f_tuple)
        assert len(years) == len(freq), "years and freq must have same length"

        # calculate linear regression
        result = linregress(years, freq)

        return tp, float(result.slope), float(result.intercept), float(result.rvalue), \
            float(result.pvalue), float(result.stderr)

    @staticmethod
    def get_freqs(row, start_year: int, end_year: int) -> List[int]:
        freqs = []
        for year in range(start_year, end_year):
            freqs.append(row[year - start_year + 2])
        return freqs
    # select lr(*) lr from (select * from schema_f limit 1)
