{{ config(
    materialized='table',
    tags=['qa'],
    post_hook=district_review_layer_hooks('district_review_layer_pieces')
) }}

-- The symmetric difference of each district present on both sides and not equal vertex
-- for vertex, as single polygons. side: 'added' if the piece is in our district and not
-- prod's, 'removed' if the reverse (decided by the piece's interior point).
--
-- is_noise: mean width (2 * area / perimeter, exact for a long thin sliver) under
-- district_review_noise_width_ft. The source's coordinates sit on a 2^-14 ft (~6.1e-5 ft)
-- grid, and two copies of the same corner differ by up to 0.0003 ft (see
-- ap_topology_node_merge_tolerance_ft), so slivers between two copies of the same line
-- are a few grid cells wide at most. 0.001 ft is about 16 cells, 3x that worst case, and
-- still far below anything a reviewer could see.
--
-- Built one layer per statement, like qa__district_review_pairs: this body is the
-- first layer, post-hooks insert the rest. The SQL is district_review_layer_pieces
-- (macros/district_review.sql).

{{ district_review_layer_pieces(district_review_selected_layers()[0]) }}
