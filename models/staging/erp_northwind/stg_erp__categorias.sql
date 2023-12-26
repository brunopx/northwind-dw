WITH 
    fonte_categories AS (
        SELECT 
            CAST(category_id AS INT) AS id_categoria
            ,CAST(category_name AS STRING) AS nome_categoria
            ,CAST(description AS STRING) AS descricao_categoria
            --,CAST(picture AS STRING) AS 
        FROM {{ source('erp', 'categories') }}
    )

SELECT * FROM fonte_categories