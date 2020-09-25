with full_rsg as (
    (
        select
            32 as res,
            rsg.label_a as label_a,
            rsg.label_b as label_b,
            least(rsg.eval.from_a.segment_b_consistency, rsg.eval.from_b.segment_a_consistency) as score,
            rsg.eval.from_a.segment_b_consistency as score_ab,
            rsg.eval.from_b.segment_a_consistency as score_ba,
            cast(32 / 8 * eval.from_a.origin.x as int64) as xa,
            cast(32 / 8 * eval.from_a.origin.y as int64) as ya,
            cast(32 / 8 * eval.from_a.origin.z as int64) as za,
            cast(32 / 8 * eval.from_b.origin.x as int64) as xb,
            cast(32 / 8 * eval.from_b.origin.y as int64) as yb,
            cast(32 / 8 * eval.from_b.origin.z as int64) as zb,
            cast(32 / 8 * point.x as int64) as x_nearby,
            cast(32 / 8 * point.y as int64) as y_nearby,
            cast(32 / 8 * point.z as int64) as z_nearby,
        from vnc_rc4.rsg32 rsg
    )
    union all
    (
        select
            16 as res,
            rsg.label_a as label_a,
            rsg.label_b as label_b,
            least(rsg.eval.from_a.segment_b_consistency, rsg.eval.from_b.segment_a_consistency) as score,
            rsg.eval.from_a.segment_b_consistency as score_ab,
            rsg.eval.from_b.segment_a_consistency as score_ba,
            cast(16 / 8 * eval.from_a.origin.x as int64) as xa,
            cast(16 / 8 * eval.from_a.origin.y as int64) as ya,
            cast(16 / 8 * eval.from_a.origin.z as int64) as za,
            cast(16 / 8 * eval.from_b.origin.x as int64) as xb,
            cast(16 / 8 * eval.from_b.origin.y as int64) as yb,
            cast(16 / 8 * eval.from_b.origin.z as int64) as zb,
            cast(16 / 8 * point.x as int64) as x_nearby,
            cast(16 / 8 * point.y as int64) as y_nearby,
            cast(16 / 8 * point.z as int64) as z_nearby,
        from vnc_rc4.rsg16 rsg
    )
    union all
    (
        select
            8 as res,
            rsg.label_a as label_a,
            rsg.label_b as label_b,
            least(rsg.eval.from_a.segment_b_consistency, rsg.eval.from_b.segment_a_consistency) as score,
            rsg.eval.from_a.segment_b_consistency as score_ab,
            rsg.eval.from_b.segment_a_consistency as score_ba,
            cast(8 / 8 * eval.from_a.origin.x as int64) as xa,
            cast(8 / 8 * eval.from_a.origin.y as int64) as ya,
            cast(8 / 8 * eval.from_a.origin.z as int64) as za,
            cast(8 / 8 * eval.from_b.origin.x as int64) as xb,
            cast(8 / 8 * eval.from_b.origin.y as int64) as yb,
            cast(8 / 8 * eval.from_b.origin.z as int64) as zb,
            cast(8 / 8 * point.x as int64) as x_nearby,
            cast(8 / 8 * point.y as int64) as y_nearby,
            cast(8 / 8 * point.z as int64) as z_nearby,
        from vnc_rc4.rsg8 rsg
    )
),
interbody_edges as (
    select
        ifnull(left_agglo.id_a, cast(rsg.label_a as int64)) as sv_a,
        ifnull(right_agglo.id_a, cast(rsg.label_b as int64)) as sv_b,
        ifnull(left_agglo.id_b, cast(rsg.label_a as int64)) as body_a,
        ifnull(right_agglo.id_b, cast(rsg.label_b as int64)) as body_b,
        *,
    from
        -- Find all edges in rsg where either side of the
        -- edge matches a segment (id_a) in the agglo mapping table.
        full_rsg rsg
        left outer join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_id_to_rep left_agglo on (rsg.label_a = left_agglo.id_a)
        left outer join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_id_to_rep right_agglo on (rsg.label_b = right_agglo.id_a)
    where
        -- But filter for only those edges where the two sides of the edge don't map to the same body,
        -- including nodes that don't map to any body at all (in which case they map to themselves.)
        -- So, the body (id_b) is null for either left (the 'a' side) or right (the 'b' side).
        -- In such cases, return both sides of the edge.
        -- (Here if both sides are mapped to the same body, this won't return anything.)
        (ifnull(left_agglo.id_b, cast(rsg.label_a as int64)) != ifnull(right_agglo.id_b, cast(rsg.label_b as int64)))
),
edges_with_info as (
    select
        *,
        mask_a.max_mask as sv_mask_a,
        mask_a.max_mask as sv_mask_b,

        btbars_a.tbar_count as body_tbars_a,
        btbars_b.tbar_count as body_tbars_b,

        stbars_a.tbar_count as sv_tbars_a,
        stbars_b.tbar_count as sv_tbars_b,

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

    from interbody_edges
        -- Some labels in rsg are not valid, but inner join with objinfo filters them out
        inner join vnc_rc4.objinfo objinfo_a on (interbody_edges.sv_a = objinfo_a.id)
        inner join vnc_rc4.objinfo objinfo_b on (interbody_edges.sv_b = objinfo_b.id)

        left join vnc_rc4.obj_to_mask mask_a on (interbody_edges.sv_a = mask_a.obj_id)
        left join vnc_rc4.obj_to_mask mask_b on (interbody_edges.sv_b = mask_b.obj_id)

        left join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_body_sizes body_sizes_a on (interbody_edges.body_a = body_sizes_a.body)
        left join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_body_sizes body_sizes_b on (interbody_edges.body_b = body_sizes_b.body)

        left join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_body_tbar_counts btbars_a on (interbody_edges.body_a = btbars_a.body)
        left join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_body_tbar_counts btbars_b on (interbody_edges.body_b = btbars_b.body)

        left join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_sv_tbar_counts stbars_a on (interbody_edges.sv_a = stbars_a.sv)
        left join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_sv_tbar_counts stbars_b on (interbody_edges.sv_b = stbars_b.sv)
),
ranked_body_scores_per_col as (
    select
        *,
        row_number() over (partition by body_a order by score desc ) as rank_a,
        row_number() over (partition by body_b order by score desc) as rank_b
    from edges_with_info
),
best_scores_per_col as (
    (
        select *, body_a as ranked_body
        from ranked_body_scores_per_col
        where rank_a <= 10
    )
    union all
    (
        select *, body_b as ranked_body
        from ranked_body_scores_per_col
        where rank_b <= 10
    )
),
ranked_body_scores as (
    select *, row_number() over (partition by ranked_body order by score desc) as rank,
    from best_scores_per_col
),
best_body_scores as (
    select *
    from ranked_body_scores
    where rank <= 10
),

filtered as (
    select
        res,
        ranked_body, rank, rank_a, rank_b,
        sv_a, sv_b,
        body_a, body_b,

        score,
        score_ab, score_ba,

        sv_mask_a, sv_mask_b,

        body_tbars_a, body_tbars_b,
        sv_tbars_a,sv_tbars_b,

        sv_size_a, sv_size_b,
        body_size_a, body_size_b,

        xa, ya, za,
        xb, yb, zb,
        x_nearby, y_nearby, z_nearby,

        body_box_x0_a, body_box_y0_a, body_box_z0_a,
        body_box_x1_a, body_box_y1_a, body_box_z1_a,
        body_box_x0_b, body_box_y0_b, body_box_z0_b,
        body_box_x1_b, body_box_y1_b, body_box_z1_b,

        sv_box_x0_a, sv_box_y0_a, sv_box_z0_a,
        sv_box_x1_a, sv_box_y1_a, sv_box_z1_a,

        sv_box_x0_b, sv_box_y0_b, sv_box_z0_b,
        sv_box_x1_b, sv_box_y1_b, sv_box_z1_b,

    from best_body_scores
        left join vnc_rc4.svs_to_keep_separate_from_eachother sep_a on (best_body_scores.body_a = sep_a.body)
        left join vnc_rc4.svs_to_keep_separate_from_eachother sep_b on (best_body_scores.body_b = sep_b.body)

        left join vnc_rc4.svs_to_keep_isolated iso_a on (best_body_scores.sv_a = iso_a.sv)
        left join vnc_rc4.svs_to_keep_isolated iso_b on (best_body_scores.sv_b = iso_b.sv)
    where
        -- Forbid excluded supervoxels from ever merging to anything
        (iso_a.sv is null and iso_b.sv is null)

        -- Forbid merges amongst the mutually exclusive body set
        and (sep_a.body is null or sep_b.body is null)

        -- Include only neuropil edges (class 5)
        -- Note: These are supervoxel mask classes, not body.
        -- {1:oob 2:trachea 3:glia 4:cell bodies 5:neuropil}
        and (sv_mask_a = 5 and sv_mask_b = 5)

        -- Score
        and (score_ab >= 0.0 and score_ba >= 0.0)
        and (score_ab <= 1.0 and score_ba <= 1.0)
        and (score_ab >= 0.0 or score_ba >= 0.0)
        and (score_ab <= 1.0 or score_ba <= 1.0)

        -- Body tbar count
        and (body_tbars_a >= 2  and body_tbars_b >= 2)
        and (body_tbars_a <= 1000000.0  and body_tbars_b <= 1000000.0)
        and (body_tbars_a >= 0 or body_tbars_b >= 0)
        and (body_tbars_a <= 1000000.0 or body_tbars_b <= 1000000.0)

        -- Supervoxel tbar count
        and (sv_tbars_a >= 0  and sv_tbars_b >= 0)
        and (sv_tbars_a <= 1000000.0  and sv_tbars_b <= 1000000.0)
        and (sv_tbars_a >= 0 or sv_tbars_b >= 0)
        and (sv_tbars_a <= 1000000.0 or sv_tbars_b <= 1000000.0)

        -- Body size
        and (body_size_a >= 0  and body_size_b >= 0)
        and (body_size_a <= 100000000000.0  and body_size_b <= 100000000000.0)
        and (body_size_a >= 0 or body_size_b >= 0)
        and (body_size_a <= 100000000000.0 or body_size_b <= 100000000000.0)

        -- Supervoxel size
        and (sv_size_a >= 0  and sv_size_b >= 0)
        and (sv_size_a <= 100000000000.0  and sv_size_b <= 100000000000.0)
        and (sv_size_a >= 0 or sv_size_b >= 0)
        and (sv_size_a <= 100000000000.0 or sv_size_b <= 100000000000.0)
)

select *
from filtered
order by
    least(body_tbars_a, body_tbars_b) desc,
    greatest(body_tbars_a, body_tbars_b),
    least(body_tbars_a, body_tbars_b),
    body_a asc, body_b asc, sv_a asc, sv_b asc
