with source as (
  select * from {{ source('raw_user_data', 'user_personal_details') }}
)

select
  id,
  loginId as login_id,
  firstName as first_name,
  lastName as last_name,
  mobileNumber as mobile_number,
  countryId as country_id,
  stateId as state_id,
  cityId as city_id,
  createdTs as created_ts,
  updatedTs as updated_ts
from source
