with source as (
  select * from {{ source('raw_user_data', 'user_account_details') }}
)

select
  id,
  loginId as login_id,
  emailId as email,
  membershipId as membership_id,
  personal_details_status,
  health_details_status,
  createdTs as created_ts,
  updatedTs as updated_ts
from source
