{{
  config(
    materialized='table',
    cluster_by=['order_id', 'review_creation_date']
  )
}}

with review_base as (
    select 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    from {{ ref('stg_order_reviews') }}
),

review_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['review_id']) }} as review_sk,
        
        -- Natural key and attributes
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from review_base
)

select 
    review_sk,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    insertion_timestamp
from review_with_sk