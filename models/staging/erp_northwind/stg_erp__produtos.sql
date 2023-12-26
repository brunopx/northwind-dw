WITH 
    fonte_produtos AS (
        SELECT 
            CAST(product_id AS INT) AS id_produto
            ,CAST(supplier_id AS INT) AS id_fornecedor
            ,CAST(category_id AS INT) AS id_categoria 
            ,CAST(product_name AS STRING) AS nome_produto
            ,CAST(quantity_per_unit AS STRING) AS quantidade_por_unidade
            ,CAST(unit_price AS DECIMAL) AS preco_por_unidade
            ,CAST(units_in_stock AS INT) AS unidades_em_estoque
            ,CAST(units_on_order AS INT) AS unidades_por_ordem
            ,CAST(reorder_level AS INT) AS nivel_reabastecimento
            ,CASE WHEN discontinued = 1 THEN TRUE ELSE FALSE END AS is_descontinuado
        FROM {{ source('erp', 'products') }}
    )

SELECT * FROM fonte_produtos