import os
import numpy as np
import pandas as pd
from tqdm import tqdm

from google.cloud import bigquery


def bq_to_df(q, client=None):
    """
    Send the given SQL query to BigQuery
    and return the results as a DataFrame.
    """
    # In theory, there are faster ways to download table data using parquet,
    # but bigquery keeps giving me errors when I try that.
    r = client.query(q).result()
    values = (row.values() for row in tqdm(r, total=r.total_rows))
    df = pd.DataFrame(values, columns=[f.name for f in r.schema])
    return df


class FocusedQuery:
    def __init__(self):
        self.params = dict(
            __BASE_SEG__ = 'vnc_rc4',

            __SCALE__ = 0,
            __RSG_TABLE__ = 'rsg8',
            __SV_OBJINFO_TABLE__ = 'objinfo',
            __SV_MASK_CLASS_TABLE__ = 'obj_to_mask',
            __SV_TBAR_COUNT_TABLE__ = 'agglo_rsg32_16_sep_8_sep1e6_sv_tbar_counts',
            __SV_EXCL_TABLE__ = 'svs_to_keep_isolated',

            __AGGLO_TABLE__ = 'agglo_rsg32_16_sep_8_sep1e6_id_to_rep',
            __AGGLO_BODY_SIZE_TABLE__ = 'agglo_rsg32_16_sep_8_sep1e6_body_sizes',
            __BODY_TBAR_COUNT_TABLE__ = 'agglo_rsg32_16_sep_8_sep1e6_body_tbar_counts',
            __SV_MUT_EXCL_TABLE__ = 'svs_to_keep_separate_from_eachother',

            __EDGE_TYPE__ = 'inter-body',
            __MIN_TWO_WAY_SCORE__ = 0.0,
            __MIN_ONE_WAY_SCORE__ = 0.0,
            __MAX_TWO_WAY_SCORE__ = 1.0,
            __MAX_ONE_WAY_SCORE__ = 1.0,

            __MIN_BODY_TBARS_BOTH__ = 0,
            __MAX_BODY_TBARS_BOTH__ = 1e6,
            __MIN_BODY_TBARS_EITHER__ = 0,
            __MAX_BODY_TBARS_EITHER__ = 1e6,

            __MIN_SV_TBARS_BOTH__ = 0,
            __MAX_SV_TBARS_BOTH__ = 1e6,
            __MIN_SV_TBARS_EITHER__ = 0,
            __MAX_SV_TBARS_EITHER__ = 1e6,

            __MIN_BODY_SIZE_BOTH__ = 0,
            __MAX_BODY_SIZE_BOTH__ = 1e11,
            __MIN_BODY_SIZE_EITHER__ = 0,
            __MAX_BODY_SIZE_EITHER__ = 1e11,

            __MIN_SV_SIZE_BOTH__ = 0,
            __MAX_SV_SIZE_BOTH__ = 1e11,
            __MIN_SV_SIZE_EITHER__ = 0,
            __MAX_SV_SIZE_EITHER__ = 1e11,
        )

    def set(self, **kwargs):
        for k, v in kwargs.items():
            sql_name = f'__{k.upper()}__'
            assert sql_name in self.params, f"{sql_name} not in known parameters"
            self.params[sql_name] = v

    def sql(self):
        query_template_path = os.path.split(__file__)[0] + '/edge_query.sql'
        with open(query_template_path, 'r') as f:
            q = ""
            for line in f.readlines():
                if not line.startswith("--!"):
                    q += line

        for k, v in self.params.items():
            q = q.replace(k, str(v))

        return q

    def query(self, client):
        return bq_to_df(self.sql(), client)


if __name__ == "__main__":
    client = bigquery.Client('janelia-flyem')
    fq = FocusedQuery()
    fq.set(rsg_table='rsg32',
           edge_type='intra-body',
           min_sv_size_both=1e6,
           min_body_size_either=1e9,
           max_two_way_score=0.1
           )

    print(fq.sql())

    # df = fq.query(client)

    # print(f"Got {len(df)} rows, starting with:")
    # print(df.head())

    # df.to_csv('/tmp/query-results.csv', header=True, index=False)
