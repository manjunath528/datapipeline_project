with source as (
  select * from {{ source('raw_user_data', 'message_center') }}
)

select
  id,
  targetLoginId as target_login_id,
  sourceLoginId as source_login_id,
  message,
  messageAcceptanceStatus as message_acceptance_status,
  status,
  isRead as is_read,
  createdTs as created_ts,
  updatedTs as updated_ts
from source
