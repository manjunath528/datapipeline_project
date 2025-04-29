with source as (
  select * from {{ source('raw_user_data', 'user_health_details') }}
)

select
  id,
  loginId as login_id,
  age,
  gender,
  height,
  currentWeight as current_weight,
  goalWeight as goal_weight,
  activityLevel as activity_level,
  targetCalories as target_calories,
  createdTs as created_ts,
  updatedTs as updated_ts
from source
