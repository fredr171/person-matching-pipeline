-- One row per live record, standardized exactly as standardization/standardize.py
-- does it (DESIGN.md section 5). Raw values are kept next to the cleaned ones.

with records as (
    select * from {{ ref('stg_current_records') }}
),

letter_map as (
    select map(list("character" order by "character"), list(replacement order by "character")) as letters
    from {{ ref('stg_ref_letters') }}
),

name_placeholders as (
    select value from {{ ref('stg_ref_placeholders') }} where field = 'name'
),

particles as (
    select particle, ends_with(particle, '''') as attached
    from {{ ref('stg_ref_particles') }}
),

particle_words as (
    select list(distinct word order by word) as words
    from (
        select unnest(string_split(particle, ' ')) as word
        from particles
        where not attached
    )
),

-- Name steps 1-4.
cleaned as (
    select
        r.*,
        r.source_system || '|' || r.source_record_id as record_key,
        r.source_system || '|' || r.source_record_id || '|' || r.record_life as record_uid,
        {{ clean_name('r.first_name', 'l.letters') }} as first_clean,
        {{ clean_name('r.last_name', 'l.letters') }} as last_clean
    from records r
    cross join letter_map l
),

-- Name step 5: a trailing suffix token, if another token remains.
suffixed as (
    select
        c.*,
        fs.canonical as first_suffix,
        ls.canonical as last_suffix,
        case when fs.canonical is null then c.first_clean
             else regexp_replace(c.first_clean, ' [^ ]+$', '') end as first_unsuffixed,
        case when ls.canonical is null then c.last_clean
             else regexp_replace(c.last_clean, ' [^ ]+$', '') end as last_unsuffixed
    from cleaned c
    left join {{ ref('stg_ref_suffixes') }} fs
        on fs.form = nullif(regexp_extract(c.first_clean, ' ([^ ]+)$', 1), '')
    left join {{ ref('stg_ref_suffixes') }} ls
        on ls.form = nullif(regexp_extract(c.last_clean, ' ([^ ]+)$', 1), '')
),

-- Name step 6: placeholders become null.
named as (
    select
        s.*,
        fp.value is not null as first_name_is_placeholder,
        lp.value is not null as last_name_is_placeholder,
        case when fp.value is null then s.first_unsuffixed end as first_std,
        case when lp.value is null then s.last_unsuffixed end as last_std
    from suffixed s
    left join name_placeholders fp on fp.value = s.first_unsuffixed
    left join name_placeholders lp on lp.value = s.last_unsuffixed
),

-- Leading particle per name (longest particle first, then alphabetical).
names_long as (
    select record_key, 'first' as field, first_std as name from named where first_std is not null
    union all
    select record_key, 'last', last_std from named where last_std is not null
),

stripped as (
    select record_key, field, stripped_name
    from (
        select
            n.record_key,
            n.field,
            case when p.attached then substr(n.name, length(p.particle) + 1)
                 else substr(n.name, length(p.particle) + 2) end as stripped_name,
            row_number() over (
                partition by n.record_key, n.field
                order by length(p.particle) desc, p.particle
            ) as particle_rank
        from names_long n
        join particles p
            on (p.attached and starts_with(n.name, p.particle) and length(n.name) > length(p.particle))
            or (not p.attached and starts_with(n.name, p.particle || ' '))
    )
    where particle_rank = 1
),

-- Comparison forms (D-030).
forms as (
    select
        n.record_key,
        n.field,
        list_sort(list_distinct(list_filter(
            [
                n.name,
                {{ plain_name('n.name') }},
                replace({{ plain_name('n.name') }}, ' ', ''),
                s.stripped_name,
                {{ plain_name('s.stripped_name') }},
                replace({{ plain_name('s.stripped_name') }}, ' ', '')
            ] || case when len(t.tokens) > 1 then t.tokens else []::varchar[] end,
            lambda f: f is not null and f <> ''
        ))) as name_forms
    from names_long n
    left join stripped s using (record_key, field)
    cross join particle_words pw
    cross join lateral (
        select list_filter(
            string_split({{ plain_name('n.name') }}, ' '),
            lambda tok: length(tok) > 1 and not list_contains(pw.words, tok)
        ) as tokens
    ) t
),

-- SSN (D-038): numeric artifacts recovered, then validity rules.
ssn_parsed as (
    select
        record_key,
        ssn as ssn_text,
        upper({{ trim_raw('ssn') }}) as s,
        regexp_matches(upper({{ trim_raw('ssn') }}), '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$') as is_scientific,
        regexp_matches(upper({{ trim_raw('ssn') }}), '^([0-9]+)\.0+$') as is_float_zero
    from cleaned
),

ssn_digits as (
    select
        record_key,
        ssn_text,
        case
            when ssn_text is null or {{ trim_raw('ssn_text') }} = '' then 'MISSING'
            when is_scientific
                 and (length(regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 2))
                          < cast(regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 3) as bigint)
                      or ltrim(substr(regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 2),
                                      cast(regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 3) as bigint) + 1),
                               '0') <> '')
                then 'UNRECOVERABLE'
        end as early_reason,
        case
            when is_scientific then
                regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 1)
                || left(regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 2),
                        cast(regexp_extract(s, '^([0-9])(?:\.([0-9]+))?E\+?([0-9]+)$', 3) as bigint))
            when is_float_zero then regexp_extract(s, '^([0-9]+)\.0+$', 1)
            else regexp_replace(s, '[^0-9]', '', 'g')
        end as digits,
        is_scientific or is_float_zero or regexp_matches(s, '^[0-9]+$') as is_numeric
    from ssn_parsed
),

ssn_std as (
    select
        d.record_key,
        case when reason is null then padded end as ssn,
        reason as ssn_null_reason
    from (
        select
            d.*,
            lpad(d.digits, 9, '0') as padded,
            case
                when d.early_reason is not null then d.early_reason
                when d.digits = '' then 'UNPARSEABLE'
                when length(d.digits) > 9 or (length(d.digits) < 9 and not d.is_numeric) then 'INVALID_LENGTH'
                when lpad(d.digits, 9, '0') in (
                    select value from {{ ref('stg_ref_placeholders') }} where field = 'ssn'
                ) then 'PLACEHOLDER'
                when starts_with(lpad(d.digits, 9, '0'), '9') then 'ITIN_OR_TEMPORARY'
                when left(lpad(d.digits, 9, '0'), 3) in ('000', '666') then 'INVALID_AREA'
                when substr(lpad(d.digits, 9, '0'), 4, 2) = '00' then 'INVALID_GROUP'
                when right(lpad(d.digits, 9, '0'), 4) = '0000' then 'INVALID_SERIAL'
            end as reason
        from ssn_digits d
    ) d
),

-- DOB (D-038).
dob_parts as (
    select
        record_key,
        dob as dob_text,
        {{ trim_raw('dob') }} as t,
        case
            when regexp_matches({{ trim_raw('dob') }}, '^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:[ T].*)?$') then
                [regexp_extract({{ trim_raw('dob') }}, '^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:[ T].*)?$', 1),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:[ T].*)?$', 2),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:[ T].*)?$', 3)]
            when regexp_matches({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$') then
                [regexp_extract({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$', 3),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$', 1),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})$', 2)]
            when regexp_matches({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{2})$') then
                [cast(2000 + cast(regexp_extract({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{2})$', 3) as integer) as varchar),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{2})$', 1),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{1,2})/([0-9]{1,2})/([0-9]{2})$', 2)]
            when regexp_matches({{ trim_raw('dob') }}, '^([0-9]{4})([0-9]{2})([0-9]{2})$') then
                [regexp_extract({{ trim_raw('dob') }}, '^([0-9]{4})([0-9]{2})([0-9]{2})$', 1),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{4})([0-9]{2})([0-9]{2})$', 2),
                 regexp_extract({{ trim_raw('dob') }}, '^([0-9]{4})([0-9]{2})([0-9]{2})$', 3)]
        end as parts
    from cleaned
),

dob_std as (
    select
        record_key,
        case when reason is null then parsed end as dob,
        reason as dob_null_reason
    from (
        select
            p.record_key,
            parsed,
            case
                when p.dob_text is null or p.t = '' then 'MISSING'
                when regexp_matches(p.t, '^[0-9]{4}$') then 'YEAR_ONLY'
                when p.parts is null then 'UNPARSEABLE'
                when parsed is null then 'INVALID_DATE'
                when {{ iso_date('parsed') }} in (
                    select value from {{ ref('stg_ref_placeholders') }} where field = 'dob'
                ) then 'PLACEHOLDER'
                when parsed > cast('{{ var("load_date") }}' as date) then 'FUTURE'
            end as reason
        from (
            select
                p.*,
                case when cast(p.parts[1] as integer) >= 1 then
                    try_cast(printf('%04d-%02d-%02d',
                                    cast(p.parts[1] as integer),
                                    cast(p.parts[2] as integer),
                                    cast(p.parts[3] as integer)) as date)
                end as parsed
            from dob_parts p
        ) p
    )
)

select
    n.source_system,
    n.source_record_id,
    n.record_life,
    n.record_key,
    n.record_uid,
    n.first_name as first_name_raw,
    n.last_name as last_name_raw,
    n.dob as dob_raw,
    n.ssn as ssn_raw,
    n.gender as gender_raw,
    n.first_std as first_name,
    n.last_std as last_name,
    coalesce(ff.name_forms, []::varchar[]) as first_name_forms,
    coalesce(lf.name_forms, []::varchar[]) as last_name_forms,
    n.first_name_is_placeholder,
    n.last_name_is_placeholder,
    coalesce(n.last_suffix, n.first_suffix) as suffix,
    s.ssn,
    s.ssn_null_reason,
    d.dob,
    d.dob_null_reason,
    coalesce(month(d.dob) = 1 and day(d.dob) = 1, false) as dob_is_jan1,
    g.gender
from named n
left join forms ff on ff.record_key = n.record_key and ff.field = 'first'
left join forms lf on lf.record_key = n.record_key and lf.field = 'last'
left join ssn_std s on s.record_key = n.record_key
left join dob_std d on d.record_key = n.record_key
left join {{ ref('stg_ref_genders') }} g on g.value = upper({{ trim_raw('n.gender') }})
