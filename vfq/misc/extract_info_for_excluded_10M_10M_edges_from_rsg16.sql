with enumerated_rsg as (
  select
    rsg.label_a as sv_a,
    rsg.label_b as sv_b,
    rsg.*,
    greatest(rsg.eval.from_a.segment_b_consistency, rsg.eval.from_b.segment_a_consistency) as score,
    row_number()
      over (
        partition by rsg.label_a, rsg.label_b
        order by greatest(rsg.eval.from_a.segment_b_consistency, rsg.eval.from_b.segment_a_consistency) desc)
      as rownum
  from vnc_rc4.rsg16 rsg
),
unique_rsg as (
  select *
  from enumerated_rsg
  where rownum = 1
),
selections as (
  (
    select
      rsg.*,
    from unique_rsg rsg
      inner join vnc_rc4_focused_exports.excluded_10M_10M_edges_from_rsg16 excl on (
        rsg.label_a = excl.sv_a and rsg.label_b = excl.sv_b
      )
    where rsg.label_a != rsg.label_b
  )
  union all
  (
    select
      rsg.*
    from unique_rsg rsg
      inner join vnc_rc4_focused_exports.excluded_10M_10M_edges_from_rsg16 excl on (
        rsg.label_a = excl.sv_b and rsg.label_b = excl.sv_a
      )
    where rsg.label_a != rsg.label_b
  )
  order by sv_a asc, sv_b asc, score desc
),

mapped as (
  select
    selections.*,
    ifnull(left_agglo.id_b, cast(selections.sv_a as int64)) as body_a,
    ifnull(right_agglo.id_b, cast(selections.sv_b as int64)) as body_b
  from selections
      -- In the agglo table, 'id_a' is the sv and 'id_b' is the body,
      -- so join on 'id_a' for both left and right.
      left outer join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_id_to_rep left_agglo on (selections.sv_a = left_agglo.id_a)
      left outer join vnc_rc4.agglo_rsg32_16_sep_8_sep1e6_id_to_rep right_agglo on (selections.sv_b = right_agglo.id_a)
),
with_info as (
  select
    mapped.*,
    obj_a.num_voxels as sv_voxels_a,
    obj_b.num_voxels as sv_voxels_b
   from mapped
    left outer join vnc_rc4.objinfo obj_a on (obj_a.id = mapped.sv_a)
    left outer join vnc_rc4.objinfo obj_b on (obj_b.id = mapped.sv_b)
),
with_mask as (
  select
    with_info.*,
    mask_a.max_mask as max_mask_a,
    mask_b.max_mask as max_mask_b,
  from with_info
    left outer join vnc_rc4.obj_to_mask mask_a on (mask_a.obj_id = with_info.sv_a)
    left outer join vnc_rc4.obj_to_mask mask_b on (mask_b.obj_id = with_info.sv_b)
)

select *
from with_mask
where
  sv_voxels_a > 1e6 and sv_voxels_b > 1e6
  and max_mask_a = 5 and max_mask_b = 5
order by body_a asc, body_b asc, sv_a asc, sv_b asc, score desc
