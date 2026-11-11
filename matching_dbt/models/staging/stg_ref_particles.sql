-- Surname particles (config/reference/particles.csv). particle_length lets standardization
-- try the longest particle first, so DE LA is stripped before DE.
select
    particle,
    length(particle) as particle_length
from {{ source('reference', 'particles') }}
