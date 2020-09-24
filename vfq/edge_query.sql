--! In the query below, replace the following names with valid table names before running the query!
--!
--! base segmentation identifier example  description
--! ============================ =======  ===========
--! __BASE_SEG__                 vnc_rc4  name of the base segmentation dataset, which
--!                                       contains all tables related to that base segmentation
--!                                       and its agglomeration(s)
--!
--!  supervoxel table identifier example                                      description
--!  =========================== ============================================ ===========
--!  __RSG_TABLE__               rsg8                                         RAG ("resegmentation table, 8nm")
--!  __SV_OBJINFO_TABLE__        objinfo                                      supervoxel location, size, etc.
--!  __SV_MASK_CLASS_TABLE__     obj_to_mask                                  SV class (most common voxel type) {1:oob 2:trachea 3:glia 4:cell bodies 5:neuropil}
--!  __SV_TBAR_COUNT_TABLE__     agglo_rsg32_16_sep_8_sep1e6_sv_tbar_counts   tbar count per supervoxel
--!  __SV_EXCL_TABLE__           svs_to_keep_isolated                         supervoxels which should never be merged to anything at all
--!
--!  agglo-specific table ids    example                                      description
--!  =========================== ============================================ ===========
--!  __AGGLO_TABLE__             agglo_rsg32_16_sep_8_sep1e6_id_to_rep        agglomeration mapping (sv -> body)
--!  __AGGLO_BODY_SIZE_TABLE__   agglo_rsg32_16_sep_8_sep1e6_body_sizes       body sizes
--!  __BODY_TBAR_COUNT_TABLE__   agglo_rsg32_16_sep_8_sep1e6_body_tbar_counts tbar count per body
--!  __SV_MUT_EXCL_TABLE__       svs_to_keep_separate_from_eachother          supervoxels (and their bodies) can participate in merges, but never into the same body
--!
--!  filtering parameter         example       description
--!  =========================   =======       ===========
--!  __EDGE_TYPE__               'inter-body'  Whether to look for edges between bodies ('inter-body') or edges within bodies ('intra-body')
--!  __MIN_TWO_WAY_SCORE__       0.1           Score must be this high in both directions (A->B and B->A)
--!  __MAX_TWO_WAY_SCORE__       1.0           Score must be this high in both directions (A->B and B->A)
--!  __MIN_ONE_WAY_SCORE__       0.5           Score must be this high in at least one direction
--!  __MAX_ONE_WAY_SCORE__       1.0           Score must be this high in at least one direction
--!
--!  __MIN_BODY_TBARS_BOTH__     2             Both bodies must have this many tbars
--!  __MAX_BODY_TBARS_BOTH__     1e6           Both bodies must have fewer tbars than this
--!  __MIN_BODY_TBARS_EITHER__   10            At least one body must have this many tbars
--!  __MAX_BODY_TBARS_EITHER__   1000          At least one body must have fewer tbars than this
--!
--!  __MIN_SV_TBARS_BOTH__       0             Both supervoxels must have this many tbars
--!  __MAX_SV_TBARS_BOTH__       1e6           Both supervoxels must have fewer tbars than this
--!  __MIN_SV_TBARS_EITHER__     0             At least one supervoxel must have this many tbars
--!  __MAX_SV_TBARS_EITHER__     1000          At least one supervoxel must have fewer tbars than this
--!
--!  __MIN_BODY_SIZE_BOTH__      1e5           Both bodies must be bigger than this
--!  __MAX_BODY_SIZE_BOTH__      1e11          Both bodies must be smaller than this
--!  __MIN_BODY_SIZE_EITHER__    1e6           At least one body must be larger than this
--!  __MAX_BODY_SIZE_EITHER__    1e10          At least one body must be smaller than this
--!
--!  __MIN_SV_SIZE_BOTH__        1e5           Both supervoxels must be bigger than this
--!  __MAX_SV_SIZE_BOTH__        1e9           Both supervoxels must be smaller than this
--!  __MIN_SV_SIZE_EITHER__      1e7           At least one supervoxel must be larger than this
--!  __MAX_SV_SIZE_EITHER__      1e8           At least one supervoxel must be smaller than this


-- Search for edges in an agglo edge table that don't
-- already exist in a flattened agglo mapping table.
-- In this example, pull edges from __RSG_TABLE__, and use
-- __AGGLO_TABLE__ as the mapping table.
-- (In the mapping table, id_a is the segment, and id_b is the body.)
--
-- TODO: Also exclude edges that we've already assigned.
with score_table as (
  select
    ifnull(left_agglo.id_a, cast(rsg.label_a as int64)) as sv_a,
    ifnull(right_agglo.id_a, cast(rsg.label_b as int64)) as sv_b,
    ifnull(left_agglo.id_b, cast(rsg.label_a as int64)) as body_a,
    ifnull(right_agglo.id_b, cast(rsg.label_b as int64)) as body_b,
    greatest(rsg.eval.from_a.segment_b_consistency, rsg.eval.from_b.segment_a_consistency) as score,
    rsg.eval.from_a.segment_b_consistency as score_ab,
    rsg.eval.from_b.segment_a_consistency as score_ba,
    power(2, __SCALE__) * rsg.eval.from_a.origin.x as xa,
    power(2, __SCALE__) * rsg.eval.from_a.origin.y as ya,
    power(2, __SCALE__) * rsg.eval.from_a.origin.z as za,
    power(2, __SCALE__) * rsg.eval.from_b.origin.x as xb,
    power(2, __SCALE__) * rsg.eval.from_b.origin.y as yb,
    power(2, __SCALE__) * rsg.eval.from_b.origin.z as zb,
    power(2, __SCALE__) * rsg.point.x as x_nearby,
    power(2, __SCALE__) * rsg.point.y as y_nearby,
    power(2, __SCALE__) * rsg.point.z as z_nearby,
  from
    -- Find all edges in __RSG_TABLE__ where either side of the
    -- edge matches a segment (id_a) in the agglo mapping table.
    __BASE_SEG__.__RSG_TABLE__ rsg
    left outer join __BASE_SEG__.__AGGLO_TABLE__ left_agglo on (rsg.label_a = left_agglo.id_a)
    left outer join __BASE_SEG__.__AGGLO_TABLE__ right_agglo on (rsg.label_b = right_agglo.id_a)

    -- Due to a bug, the RSG tables contain some supervoxel IDs that don't really exist.
    -- We can filter those out via an inner join on the objinfo table, which only contains valid IDs.
    inner join vnc_rc4.objinfo objinfo_a on (rsg.label_a = objinfo_a.id)
    inner join vnc_rc4.objinfo objinfo_b on (rsg.label_b = objinfo_b.id)
  where
    -- But filter for only those edges where the two sides of the edge don't map to the same body,
    -- including nodes that don't map to any body at all (in which case they map to themselves.)
    -- So, the body (id_b) is null for either left (the 'a' side) or right (the 'b' side).
    -- In such cases, return both sides of the edge.
    -- (Here if both sides are mapped to the same body, this won't return anything.)
    ("__EDGE_TYPE__" = "inter-body" and ifnull(left_agglo.id_b, cast(rsg.label_a as int64)) != ifnull(right_agglo.id_b, cast(rsg.label_b as int64)))
    -- Or filter for edges where the two supervoxels ARE in the same body.
    or
    ("__EDGE_TYPE__" = "intra-body"
    and (rsg.label_a != rsg.label_b) -- Apparently RSG can contain self-edges. Drop them.
    and ifnull(left_agglo.id_b, cast(rsg.label_a as int64)) = ifnull(right_agglo.id_b, cast(rsg.label_b as int64)))
),
filtered as (
  select
    sv_a, sv_b,
    body_a, body_b,
    score, score_ab, score_ba,

    mask_a.max_mask as sv_mask_a,
    mask_a.max_mask as sv_mask_b,

    btbars_a.tbar_count as body_tbars_a,
    btbars_b.tbar_count as body_tbars_b,

    stbars_a.tbar_count as sv_tbars_a,
    stbars_b.tbar_count as sv_tbars_b,

    xa, ya, za,
    xb, yb, zb,
    x_nearby, y_nearby, z_nearby,

    body_sizes_a.body_size as body_size_a,
    body_sizes_b.body_size as body_size_b,

    body_sizes_a.box_x0 as body_box_x0_a,
    body_sizes_a.box_y0 as body_box_y0_a,
    body_sizes_a.box_z0 as body_box_z0_a,

    body_sizes_a.box_x1 as body_box_x1_a,
    body_sizes_a.box_y1 as body_box_y1_a,
    body_sizes_a.box_z1 as body_box_z1_a,

    body_sizes_b.box_x0 as body_box_x0_b,
    body_sizes_b.box_y0 as body_box_y0_b,
    body_sizes_b.box_z0 as body_box_z0_b,

    body_sizes_b.box_x1 as body_box_x1_b,
    body_sizes_b.box_y1 as body_box_y1_b,
    body_sizes_b.box_z1 as body_box_z1_b,

    objinfo_a.num_voxels as sv_size_a,
    objinfo_b.num_voxels as sv_size_b,

    objinfo_a.bbox.start.x as sv_box_x0_a,
    objinfo_a.bbox.start.y as sv_box_y0_a,
    objinfo_a.bbox.start.z as sv_box_z0_a,

    (objinfo_a.bbox.start.x + objinfo_a.bbox.size.x) as sv_box_x1_a,
    (objinfo_a.bbox.start.y + objinfo_a.bbox.size.y) as sv_box_y1_a,
    (objinfo_a.bbox.start.z + objinfo_a.bbox.size.z) as sv_box_z1_a,

    objinfo_b.bbox.start.x as sv_box_x0_b,
    objinfo_b.bbox.start.y as sv_box_y0_b,
    objinfo_b.bbox.start.z as sv_box_z0_b,

    (objinfo_b.bbox.start.x + objinfo_b.bbox.size.x) as sv_box_x1_b,
    (objinfo_b.bbox.start.y + objinfo_b.bbox.size.y) as sv_box_y1_b,
    (objinfo_b.bbox.start.z + objinfo_b.bbox.size.z) as sv_box_z1_b,

  from score_table
    left join __BASE_SEG__.__SV_OBJINFO_TABLE__ objinfo_a on (score_table.sv_a = objinfo_a.id)
    left join __BASE_SEG__.__SV_OBJINFO_TABLE__ objinfo_b on (score_table.sv_b = objinfo_b.id)

    left join __BASE_SEG__.__SV_MASK_CLASS_TABLE__ mask_a on (score_table.sv_a = mask_a.obj_id)
    left join __BASE_SEG__.__SV_MASK_CLASS_TABLE__ mask_b on (score_table.sv_b = mask_b.obj_id)

    left join __BASE_SEG__.__AGGLO_BODY_SIZE_TABLE__ body_sizes_a on (score_table.body_a = body_sizes_a.body)
    left join __BASE_SEG__.__AGGLO_BODY_SIZE_TABLE__ body_sizes_b on (score_table.body_b = body_sizes_b.body)

    left join __BASE_SEG__.__BODY_TBAR_COUNT_TABLE__ btbars_a on (score_table.body_a = btbars_a.body)
    left join __BASE_SEG__.__BODY_TBAR_COUNT_TABLE__ btbars_b on (score_table.body_b = btbars_b.body)

    left join __BASE_SEG__.__SV_TBAR_COUNT_TABLE__ stbars_a on (score_table.sv_a = stbars_a.sv)
    left join __BASE_SEG__.__SV_TBAR_COUNT_TABLE__ stbars_b on (score_table.sv_b = stbars_b.sv)

    left join __BASE_SEG__.__SV_MUT_EXCL_TABLE__ sep_a on (score_table.body_a = sep_a.body)
    left join __BASE_SEG__.__SV_MUT_EXCL_TABLE__ sep_b on (score_table.body_b = sep_b.body)

    left join __BASE_SEG__.__SV_EXCL_TABLE__ iso_a on (score_table.sv_a = iso_a.sv)
    left join __BASE_SEG__.__SV_EXCL_TABLE__ iso_b on (score_table.sv_b = iso_b.sv)

  where
    -- Forbid excluded supervoxels from ever merging to anything
    (iso_a.sv is null and iso_b.sv is null)

    -- Forbid merges amongst the mutually exclusive body set
    and (sep_a.body is null or sep_b.body is null)

    -- Include only neuropil edges (class 5)
    -- Note: These are supervoxel mask classes
    -- TODO: Use body masks!
    -- {1:oob 2:trachea 3:glia 4:cell bodies 5:neuropil}
    and (mask_a.max_mask = 5 and mask_b.max_mask = 5)

    -- Score
    and (score_ab >= __MIN_TWO_WAY_SCORE__ and score_ba >= __MIN_TWO_WAY_SCORE__)
    and (score_ab <= __MAX_TWO_WAY_SCORE__ and score_ba <= __MAX_TWO_WAY_SCORE__)
    and (score_ab >= __MIN_ONE_WAY_SCORE__ or score_ba >= __MIN_ONE_WAY_SCORE__)
    and (score_ab <= __MAX_ONE_WAY_SCORE__ or score_ba <= __MAX_ONE_WAY_SCORE__)

    -- Body tbar count
    and (btbars_a.tbar_count >= __MIN_BODY_TBARS_BOTH__  and btbars_b.tbar_count >= __MIN_BODY_TBARS_BOTH__)
    and (btbars_a.tbar_count <= __MAX_BODY_TBARS_BOTH__  and btbars_b.tbar_count <= __MAX_BODY_TBARS_BOTH__)
    and (btbars_a.tbar_count >= __MIN_BODY_TBARS_EITHER__ or btbars_b.tbar_count >= __MIN_BODY_TBARS_EITHER__)
    and (btbars_a.tbar_count <= __MAX_BODY_TBARS_EITHER__ or btbars_b.tbar_count <= __MAX_BODY_TBARS_EITHER__)

    -- Supervoxel tbar count
    and (stbars_a.tbar_count >= __MIN_SV_TBARS_BOTH__  and stbars_b.tbar_count >= __MIN_SV_TBARS_BOTH__)
    and (stbars_a.tbar_count <= __MAX_SV_TBARS_BOTH__  and stbars_b.tbar_count <= __MAX_SV_TBARS_BOTH__)
    and (stbars_a.tbar_count >= __MIN_SV_TBARS_EITHER__ or stbars_b.tbar_count >= __MIN_SV_TBARS_EITHER__)
    and (stbars_a.tbar_count <= __MAX_SV_TBARS_EITHER__ or stbars_b.tbar_count <= __MAX_SV_TBARS_EITHER__)

    -- Body size
    and (body_sizes_a.body_size >= __MIN_BODY_SIZE_BOTH__  and body_sizes_b.body_size >= __MIN_BODY_SIZE_BOTH__)
    and (body_sizes_a.body_size <= __MAX_BODY_SIZE_BOTH__  and body_sizes_b.body_size <= __MAX_BODY_SIZE_BOTH__)
    and (body_sizes_a.body_size >= __MIN_BODY_SIZE_EITHER__ or body_sizes_b.body_size >= __MIN_BODY_SIZE_EITHER__)
    and (body_sizes_a.body_size <= __MAX_BODY_SIZE_EITHER__ or body_sizes_b.body_size <= __MAX_BODY_SIZE_EITHER__)

    -- Supervoxel size
    and (objinfo_a.num_voxels >= __MIN_SV_SIZE_BOTH__  and objinfo_b.num_voxels >= __MIN_SV_SIZE_BOTH__)
    and (objinfo_a.num_voxels <= __MAX_SV_SIZE_BOTH__  and objinfo_b.num_voxels <= __MAX_SV_SIZE_BOTH__)
    and (objinfo_a.num_voxels >= __MIN_SV_SIZE_EITHER__ or objinfo_b.num_voxels >= __MIN_SV_SIZE_EITHER__)
    and (objinfo_a.num_voxels <= __MAX_SV_SIZE_EITHER__ or objinfo_b.num_voxels <= __MAX_SV_SIZE_EITHER__)
),
enumerated_pairs as (
  select *,
    row_number()
      over (
        partition by filtered.sv_a, filtered.sv_b
        order by filtered.score desc
      ) as rownum
  from filtered
),
unique_pairs as (
  select *
  from enumerated_pairs
  where rownum = 1
)
select *
from unique_pairs
