WITH 
    ordens AS (
        SELECT *
        FROM {{ ref('stg_erp__ordens') }}
    )

    ,ordem_detalhes AS (
        SELECT *
        FROM {{ ref('stg_erp__ordem_detalhes') }}
    )

    ,join_tabelas AS (
        SELECT 
            ordens.id_pedido
            ,ordem_detalhes.id_produto
            ,ordens.id_funcionario
            ,ordens.id_cliente
            ,ordens.id_transportadora
            ,ordens.data_do_pedido
            ,ordens.frete
            ,ordens.destinatario
            ,ordens.endereco_destinatario
            ,ordens.cep_destinatario
            ,ordens.cidade_destinatario
            ,ordens.regiao_destinatario
            ,ordens.pais_destinatario
            ,ordens.data_do_envio
            ,ordens.data_requerida_entrega
            ,ordem_detalhes.desconto_perc
            ,ordem_detalhes.preco_da_unidade
            ,ordem_detalhes.quantidade
        FROM
        ordem_detalhes
        LEFT JOIN ordens ON
            (ordens.id_pedido = ordem_detalhes.id_pedido)
    )

    ,criar_chave AS (
        SELECT 
            CAST (id_pedido AS STRING) || CAST (id_produto AS STRING) AS sk_pedido_item
            ,*
        FROM join_tabelas
    )

    SELECT * FROM criar_chave