{% macro nb_decklists_macro(grain) %}
select {{ grain }}, count(*)::int as nb_decklists
from {{ ref("decklists_aggregated") }} da
inner join {{ ref("tournaments") }} t on da.tournament_id = t.tournament_id
group by {{ grain }}
{% endmacro %}