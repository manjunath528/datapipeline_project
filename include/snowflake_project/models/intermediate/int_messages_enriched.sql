with msg as (
  select * from {{ ref('stg_message_center') }}
),
source_user as (
  select login_id, first_name, last_name from {{ ref('int_user_profiles') }}
),
target_user as (
  select login_id, first_name, last_name from {{ ref('int_user_profiles') }}
)

select
  msg.id,
  msg.message,
  msg.status,
  msg.is_read,
  msg.message_acceptance_status,
  msg.created_ts,
  su.first_name as source_first_name,
  su.last_name as source_last_name,
  tu.first_name as target_first_name,
  tu.last_name as target_last_name
from msg
left join source_user su on msg.source_login_id = su.login_id
left join target_user tu on msg.target_login_id = tu.login_id

