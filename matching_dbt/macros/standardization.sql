{#-
  SQL forms of the rules in standardization/standardize.py (DESIGN.md section 5).
  Each macro mirrors one Python function; tests/test_dbt.py checks they agree on
  every generated record.
-#}

{#- Name steps 1-4: NFC, letter table, drop marks, apostrophes, ASCII, uppercase,
    punctuation, hyphens/apostrophes only between letters or digits, spaces. -#}
{% macro clean_name(expr, letter_map) -%}
nullif(trim(regexp_replace(
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(regexp_replace(
            translate(
                regexp_replace(
                    array_to_string(
                        list_transform(string_split(nfc_normalize({{ expr }}), ''),
                                       lambda ch: coalesce({{ letter_map }}[ch], ch)),
                        ''),
                    '[\x{0300}-\x{036F}]', '', 'g'),
                chr(8217) || chr(8216) || chr(700) || chr(96) || chr(180),
                ''''''''''''),
            '[^\x00-\x7F]', ' ', 'g')),
        '[^A-Z0-9'' -]', ' ', 'g'),
        '[''-]{2,}', ' ', 'g'),
        '(^|[^A-Z0-9])[''-]', '\1 ', 'g'),
        '[''-]($|[^A-Z0-9])', ' \1', 'g'),
    ' +', ' ', 'g'), ' '), '')
{%- endmacro %}

{#- Without apostrophes, with hyphens as spaces. -#}
{% macro plain_name(expr) -%}
trim(regexp_replace(replace(replace({{ expr }}, '''', ''), '-', ' '), ' +', ' ', 'g'), ' ')
{%- endmacro %}

{#- Raw SSN/DOB/gender values are trimmed of ASCII whitespace only. -#}
{% macro trim_raw(expr) -%}
trim({{ expr }}, ' ' || chr(9) || chr(13) || chr(10))
{%- endmacro %}

{#- YYYY-MM-DD with a zero-padded year. -#}
{% macro iso_date(expr) -%}
printf('%04d-%02d-%02d', year({{ expr }}), month({{ expr }}), day({{ expr }}))
{%- endmacro %}

{% macro year_text(expr) -%}
printf('%04d', year({{ expr }}))
{%- endmacro %}

{#- Whole years from the earlier date to the later one, age-style. -#}
{% macro years_between(a, b) -%}
(year(greatest({{ a }}, {{ b }})) - year(least({{ a }}, {{ b }}))
 - case when month(greatest({{ a }}, {{ b }})) * 100 + day(greatest({{ a }}, {{ b }}))
            < month(least({{ a }}, {{ b }})) * 100 + day(least({{ a }}, {{ b }}))
        then 1 else 0 end)
{%- endmacro %}

{#- Number of positions where two equal-length strings differ. -#}
{% macro hamming(a, b, length) -%}
len(list_filter(range(1, {{ length + 1 }}), lambda i: substr({{ a }}, i, 1) <> substr({{ b }}, i, 1)))
{%- endmacro %}
