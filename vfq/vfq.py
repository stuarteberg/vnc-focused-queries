import os
import json
import shutil
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
        self.order_by = ""
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
            if k.startswith('__'):
                sql_name = k
            else:
                sql_name = f'__{k.upper()}__'

            assert sql_name in self.params, f"{sql_name} not in known parameters"
            self.params[sql_name] = v

            # The scale should be consistent with the RSG used.
            # (Should we just assert that the caller always sets both at the same time?)
            if k.lower() == 'rsg_table':
                scale = int(np.log2(int(v[3:]) // 8))
                self.params['__SCALE__'] = scale

    def set_order_by(self, order_by):
        self.order_by = order_by

    def sql(self):
        query_template_path = os.path.split(__file__)[0] + '/edge_query.sql'
        with open(query_template_path, 'r') as f:
            q = ""
            for line in f.readlines():
                if not line.startswith("--!"):
                    q += line

        for k, v in self.params.items():
            q = q.replace(k, str(v))

        q += f"\norder by {self.order_by}\n"

        return q

    def query(self, client):
        return bq_to_df(self.sql(), client)

    def query_and_export(self, dirname, client, overwrite=False):
        if os.path.exists(dirname) and not overwrite:
            raise RuntimeError("Directory '{dirname}' already exists and you didn't set overwrite=True")

        df = self.query(client)
        print(f"Got {len(df)} rows, starting with:")
        print(df.head())

        if os.path.exists(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)
        with open(f'{dirname}/params.json', 'w') as f:
            json.dump(self.params, f, indent=2)
        with open(f'{dirname}/query.sql', 'w') as f:
            f.write(self.sql())

        df.to_csv(f'{dirname}/results.csv', header=True, index=False)


if __name__ == "__main__":
    client = bigquery.Client('janelia-flyem')

    for rsg in ['rsg32', 'rsg16', 'rsg8']:
        print(f"Running query for {rsg}")
        fq = FocusedQuery()
        fq.set(rsg_table=rsg,
               edge_type='intra-body',
               min_sv_size_both=1e6,
               min_body_size_either=1e9)

        fq.set_order_by("least(sv_tbars_a, sv_tbars_b) desc, body_a asc, body_b asc, sv_a asc, sv_b asc")
        #print(fq.sql())
        fq.query_and_export(f"{rsg}-intrabody", client)

    # df = fq.query(client)

    # print(f"Got {len(df)} rows, starting with:")
    # print(df.head())

    # df.to_csv('/tmp/query-results.csv', header=True, index=False)
