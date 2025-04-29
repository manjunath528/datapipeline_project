with acc as (
  select * from {{ ref('stg_user_account_details') }}
),
pers as (
  select * from {{ ref('stg_user_personal_details') }}
),
health as (
  select * from {{ ref('stg_user_health_details') }}
)

select
  acc.login_id,
  acc.email,
  acc.membership_id,
  acc.personal_details_status,
  acc.health_details_status,
  pers.first_name,
  pers.last_name,
  pers.mobile_number,
  pers.country_id,
  pers.state_id,
  pers.city_id,
  health.age,
  health.gender,
  health.height,
  health.current_weight,
  health.goal_weight,
  health.activity_level,
  health.target_calories,
  acc.created_ts
from acc
left join pers using (login_id)
left join health using (login_id)
